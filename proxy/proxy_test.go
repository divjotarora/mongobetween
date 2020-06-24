package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/version"
	"go.uber.org/zap"
)

var (
	ctx       = context.Background()
	proxyPort = 33000
	proxyURI  = fmt.Sprintf("mongodb://localhost:%d/test", proxyPort)
)

type Trainer struct {
	Name string
	Age  int
	City string
}

func TestProxy(t *testing.T) {
	proxy := setupProxy(t, true)

	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	client := setupClient(t)
	collection := client.Database("test").Collection("trainers")
	_, err := collection.DeleteMany(ctx, bson.D{{}})
	assert.Nil(t, err)

	ash := Trainer{"Ash", 10, "Pallet Town"}
	misty := Trainer{"Misty", 10, "Cerulean City"}
	brock := Trainer{"Brock", 15, "Pewter City"}

	_, err = collection.InsertOne(ctx, ash)
	assert.Nil(t, err)

	_, err = collection.InsertMany(ctx, []interface{}{misty, brock})
	assert.Nil(t, err)

	filter := bson.D{{Key: "name", Value: "Ash"}}
	update := bson.D{
		{Key: "$inc", Value: bson.D{
			{Key: "age", Value: 1},
		}},
	}
	updateResult, err := collection.UpdateOne(ctx, filter, update)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), updateResult.MatchedCount)
	assert.Equal(t, int64(1), updateResult.ModifiedCount)

	var result Trainer
	err = collection.FindOne(ctx, filter).Decode(&result)
	assert.Nil(t, err)
	assert.Equal(t, "Pallet Town", result.City)

	var results []Trainer
	cur, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(2).SetBatchSize(1))
	assert.Nil(t, err)
	err = cur.All(ctx, &results)
	assert.Nil(t, err)
	assert.Equal(t, "Pallet Town", results[0].City)
	assert.Equal(t, "Cerulean City", results[1].City)

	deleteResult, err := collection.DeleteMany(ctx, bson.D{{}})
	assert.Nil(t, err)
	assert.Equal(t, int64(3), deleteResult.DeletedCount)

	err = client.Disconnect(ctx)
	assert.Nil(t, err)

	proxy.Shutdown()
}

func TestProxyUnacknowledgedWrites(t *testing.T) {
	proxy := setupProxy(t, true)
	defer proxy.Shutdown()

	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	// Create a client with retryable writes disabled so the test will fail if the proxy crashes while processing the
	// unacknowledged write. If the proxy were to crash, it would close all connections and the next write would error
	// if retryable writes are disabled.
	clientOpts := options.Client().SetRetryWrites(false)
	client := setupClient(t, clientOpts)
	defer func() {
		err := client.Disconnect(ctx)
		assert.Nil(t, err)
	}()

	// Create two *Collection instances: one for setup and basic operations and and one configured with an
	// unacknowledged write concern for testing.
	wc := writeconcern.New(writeconcern.W(0))
	setupCollection := client.Database("test").Collection("trainers")
	unackCollection, err := setupCollection.Clone(options.Collection().SetWriteConcern(wc))
	assert.Nil(t, err)

	// Setup by deleteing all documents.
	_, err = setupCollection.DeleteMany(ctx, bson.D{})
	assert.Nil(t, err)

	ash := Trainer{"Ash", 10, "Pallet Town"}
	_, err = unackCollection.InsertOne(ctx, ash)
	assert.Equal(t, mongo.ErrUnacknowledgedWrite, err) // driver returns a special error value for w=0 writes

	// Insert a document using the setup collection and ensure document count is 2. Doing this ensures that the proxy
	// did not crash while processing the unacknowledged write.
	_, err = setupCollection.InsertOne(ctx, ash)
	assert.Nil(t, err)

	count, err := setupCollection.CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)
}

func TestShutdown(t *testing.T) {
	log.Printf("testing against driver version %s\n", version.Driver)

	// High HearbeatInterval: prevents proxy from discovering new topology state after mongos is shutdown
	// Low server selection timeout: Fail fast if no nodes are available. This should only happen if testing against a
	// standalone. In the sharded case, there are two mongos's so all requests should go to the other.
	proxyOpts := options.Client().
		ApplyURI("mongodb://localhost:27017,localhost:27018").
		SetHeartbeatInterval(60 * time.Second).
		SetServerSelectionTimeout(2 * time.Second)
	proxy := setupProxy(t, false, proxyOpts)
	defer proxy.Shutdown()

	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	// High HeartbeatInterval: prevent unnecessary heartbeats to the mongobetween.
	clientOpts := options.Client().
		SetHeartbeatInterval(60 * time.Second).
		SetServerSelectionTimeout(2 * time.Second).
		SetRetryReads(false)
	client := setupClient(t, clientOpts)
	defer client.Disconnect(ctx)

	shutdownMongos(t, "mongodb://localhost:27017")

	var idx int
	ticker := time.NewTicker(5 * time.Second)
	coll := client.Database("foo").Collection("bar")

	// Send requests for five seconds. The first must fail and the subsequent ones must succeed because the proxy
	// should re-discover the topology on the first failure.
	for {
		select {
		case <-ticker.C:
			return
		default:
		}

		_, err := coll.CountDocuments(ctx, bson.D{})
		if idx == 0 {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}

		time.Sleep(500 * time.Millisecond)
		idx++
	}
}

func shutdownMongos(t *testing.T, mongosURI string) {
	t.Helper()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongosURI).SetServerSelectionTimeout(500*time.Millisecond))
	assert.Nil(t, err)
	defer client.Disconnect(ctx)

	admin := client.Database("admin")
	cmd := bson.M{"shutdown": 1}
	err = admin.RunCommand(ctx, cmd).Err()
	require.NotNil(t, err)

	ce, ok := err.(mongo.CommandError)
	require.True(t, ok, "expected err of type %T, got %v of type %T", mongo.CommandError{}, err, err)
	require.True(t, ce.HasErrorLabel("NetworkError"), "expected network error, got %v", err)
}

func initZapLog(t *testing.T) *zap.Logger {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "message"
	log, err := config.Build(zap.AddStacktrace(zap.FatalLevel))
	require.Nil(t, err)
	return log
}

func setupProxy(t *testing.T, ping bool, opts ...*options.ClientOptions) *Proxy {
	t.Helper()

	uri := "mongodb://localhost:27017/test"
	if os.Getenv("CI") == "true" {
		uri = "mongodb://mongo:27017/test"
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	uriOpts := options.Client().ApplyURI(uri)
	allClientOpts := append([]*options.ClientOptions{uriOpts}, opts...)
	mergedClientOpts := options.MergeClientOptions(allClientOpts...)

	proxy, err := NewProxy(initZapLog(t), sd, "label", "tcp4", fmt.Sprintf(":%d", proxyPort), false, ping, mergedClientOpts)
	assert.Nil(t, err)
	return proxy
}

func setupClient(t *testing.T, clientOpts ...*options.ClientOptions) *mongo.Client {
	t.Helper()

	// Base options should only use ApplyURI. The full set should have the user-supplied options after uriOpts so they
	// will win out in the case of conflicts.
	uriOpts := options.Client().ApplyURI(proxyURI)
	allClientOpts := append([]*options.ClientOptions{uriOpts}, clientOpts...)

	client, err := mongo.Connect(ctx, allClientOpts...)
	assert.Nil(t, err)

	// Call Ping with a low timeout to ensure the cluster is running and fail-fast if not.
	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	err = client.Ping(pingCtx, nil)
	if err != nil {
		// Clean up in failure cases.
		_ = client.Disconnect(ctx)

		// Use t.Fatalf instead of assert because we want to fail fast if the cluster is down.
		t.Fatalf("error pinging cluster: %v", err)
	}

	return client
}
