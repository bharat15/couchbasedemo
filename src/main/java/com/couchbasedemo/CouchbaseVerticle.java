package com.couchbasedemo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.CouchbaseAsyncCluster;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import rx.schedulers.Schedulers;

public class CouchbaseVerticle extends AbstractVerticle
{
    private static final Logger LOGGER =  LoggerFactory.getLogger(CouchbaseVerticle.class);
    
    private CouchbaseAsyncCluster cluster;
    private volatile AsyncBucket bucket;
    
    @Override
    public void init(Vertx vertx, Context context) {
    	
    	super.init(vertx, context);
    	
    	//getting the configuration json
    	JsonObject config = context.config();
    	
    	//getting the bootstrap node, as a JSON array
    	JsonArray seedNodeArray = config.getJsonArray("couchbase.seedNodes", new JsonArray().add("localhost")); 
    			
    	//convert to list
    	List seedNodes = new ArrayList&lt;&gt;(seedNodeArray.size());
    	for(Object seedNode: seedNodeArray) {
    		seedNodes.add((String) seedNode);
    	}
    	//use that to bootstrap a cluster
    	this.cluster = CouchbaseAsyncCluster.create(seedNodes);
    }
    
    public static void main(String[] args) throws InterruptedException {
        Vertx vertx = Vertx.vertx();

        final CountDownLatch startLatch = new CountDownLatch(1);
        vertx.deployVerticle(new CouchbaseVerticle(), event -&gt; {
            if (event.succeeded())
                LOGGER.info("Verticle Deployed - " + event.result());
            else
                LOGGER.error("Verticle deployment error", event.cause());
            startLatch.countDown();
        });
        startLatch.await();

        final CountDownLatch stopLatch = new CountDownLatch(1);
        vertx.close(event -&gt; {
            if (event.succeeded())
                LOGGER.info("Vert.x Stopped - " + event.result());
            else
                LOGGER.error("Vert.x stopping error", event.cause());
            stopLatch.countDown();
        });
        stopLatch.await();
    }
    
    @Override
    public void start(Future<Void> startFuture) throws Exception{
    	
    	cluster.openBucket(config().getString("couchbase.bucketName","default"), config().getString("couchbase.bucketPassword",""))
    		.doOnNext(openedBucket -&gt; LOGGER.info("Bucket opened " + openedBucket.name()))
    		.subscribe(
    				openedBucket -&gt; bucket = openedBucket,
                    startFuture::fail,
                    startFuture::complete);
    }
    
    @Override
    public void stop(Future stopFuture) throws Exception {
        cluster.disconnect()
                .doOnNext(isDisconnectedCleanly -&gt; LOGGER.info("Disconnected Cluster (cleaned threads: " + isDisconnectedCleanly + ")"))
                .subscribe(
                        isDisconnectedCleanly -&gt; stopFuture.complete(),
                        stopFuture::fail,
                        Schedulers::shutdown);
    }
    
}
