/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.mongodb;

import com.mongodb.*;
import org.bson.types.ObjectId;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.util.concurrent.jsr166y.TransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 */

public class MongoDBRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final String riverIndexName;

    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDb;
    private final String mongoCollection;
    private final String mongoFilter;
    private final String mongoUser;
    private final String mongoPassword;

    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private final TimeValue bulkTimeout;

    private final ExecutableScript script;
    private final Map<String, Object> scriptParams = Maps.newHashMap();

    private volatile Thread tailerThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    private final TransferQueue<Map> stream = new LinkedTransferQueue<Map>();

    @Inject public MongoDBRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        if (settings.settings().containsKey("mongodb")) {
            Map<String, Object> mongoSettings = (Map<String, Object>) settings.settings().get("mongodb");
            mongoHost = XContentMapValues.nodeStringValue(mongoSettings.get("host"), "localhost");
            mongoPort = XContentMapValues.nodeIntegerValue(mongoSettings.get("port"), 27017);
            mongoDb = XContentMapValues.nodeStringValue(mongoSettings.get("db"), riverName.name());
            mongoCollection = XContentMapValues.nodeStringValue(mongoSettings.get("collection"), riverName.name());
            mongoFilter = XContentMapValues.nodeStringValue(mongoSettings.get("filter"), null);
            if (mongoSettings.containsKey("user") && mongoSettings.containsKey("password")) {
                mongoUser = mongoSettings.get("user").toString();
                mongoPassword = mongoSettings.get("password").toString();
            }else{
                mongoUser = "";
                mongoPassword = "";
                
            }

            if (mongoSettings.containsKey("script")) {
                script = scriptService.executable("js", mongoSettings.get("script").toString(), Maps.newHashMap());
            } else {
                script = null;
            }
        } else {
            mongoHost = "localhost";
            mongoPort = 27017;
            mongoDb = riverName.name();
            mongoFilter = null;
            mongoCollection = riverName.name();
            mongoUser = "";
            mongoPassword = "";
            script = null;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), mongoDb);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), mongoDb);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
        } else {
            indexName = mongoDb;
            typeName = mongoDb;
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
        }
    }

    @Override public void start() {
        logger.info("starting mongodb stream: host [{}], port [{}], filter [{}], db [{}], indexing to [{}]/[{}]", mongoHost, mongoPort, mongoFilter, mongoDb, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        tailerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_slurper").newThread(new Tailer());
        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_indexer").newThread(new Indexer());
        indexerThread.start();
        tailerThread.start();
    }

    @Override public void close() {
        if (closed) {
            return;
        }
        logger.info("closing mongodb stream river");
        tailerThread.interrupt();
        indexerThread.interrupt();
        closed = true;
    }

    private class Indexer implements Runnable {
        @Override public void run() {
            while (true) {
                if (closed) {
                    return;
                }

                Map data = null;
                try {
                    data = stream.take();
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                    continue;
                }
                BulkRequestBuilder bulk = client.prepareBulk();
                XContentBuilder builder;
                String lastId = null;
                try {
                    builder = XContentFactory.jsonBuilder().map(data);
                    bulk.add(indexRequest(indexName).type(typeName).id(data.get("_id").toString()).source(builder));
                    lastId = data.get("_id").toString();
                } catch (IOException e) {
                    logger.warn("failed to parse {}", e, data);
                }

                // spin a bit to see if we can get some more changes
                try {
                    while ((data = stream.poll(bulkTimeout.millis(), TimeUnit.MILLISECONDS)) != null) {
                        try {
                            builder = XContentFactory.jsonBuilder().map(data);
                            bulk.add(indexRequest(indexName).type(typeName).id(data.get("_id").toString()).source(builder));
                            lastId = data.get("_id").toString();
                        } catch (IOException e) {
                            logger.warn("failed to parse {}", e, data);
                        }

                        if (bulk.numberOfActions() >= bulkSize) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

                if (lastId != null) {
                    try {
                        bulk.add(indexRequest(riverIndexName).type(riverName.name()).id("_last_id")
                                .source(jsonBuilder().startObject().startObject("mongodb").field("last_id", lastId).endObject().endObject()));
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }

                try {
                    BulkResponse response = bulk.execute().actionGet();
                    if (response.hasFailures()) {
                        // TODO write to exception queue?
                        logger.warn("failed to execute" + response.buildFailureMessage());
                    }
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                }
            }
        }
    }


    private class Tailer implements Runnable {
        @Override public void run() {

            // There should be just one Mongo instance per jvm
            // it has an internal connection pooling. In this case
            // we'll use a single Mongo to handle the river.
            Mongo m = null;
            try {
                ArrayList<ServerAddress> addr = new ArrayList<ServerAddress>();
                addr.add(new ServerAddress(mongoHost, mongoPort));
                m = new Mongo(addr);
                m.slaveOk();
            } catch (UnknownHostException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } 
            
            while (true) {
                if (closed) {
                    return;
                }
                
                try {

                    DB db = m.getDB( mongoDb );
                    if (!mongoUser.isEmpty() && !mongoPassword.isEmpty()){
                        boolean auth = db.authenticate(mongoUser, mongoPassword.toCharArray());
                        if (auth==false){
                            logger.warn("Invalid credential");
                            break;
                        }
                    }


                String lastId = null;
                try {
                    client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                    GetResponse lastIdResponse = client.prepareGet(riverIndexName, riverName().name(), "_last_id").execute().actionGet();
                    if (lastIdResponse.exists()) {
                        Map<String, Object> mongodbState = (Map<String, Object>) lastIdResponse.sourceAsMap().get("mongodb");
                        if (mongodbState != null) {
                            lastId = mongodbState.get("last_id").toString();
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to get last_id", e);
                     try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                }

                    DBCollection coll = db.getCollection(mongoCollection); 
                    DBCursor cur = coll.find();

                    if (lastId != null) {
                        // The $gte filter is useful for capped collections. It allow us to initialize the tail.
                        // See docs.
                        cur = coll.find(new BasicDBObject("_id", new BasicDBObject("$gte", new ObjectId(lastId))));
                    }

                    try {

                        // If coll is capped then use a tailable cursor and sort by $natural
                        if (coll.isCapped())
                        {
                            cur = cur.sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE);
                            logger.debug("Collection is capped adding tailable option");
                        } else {
                            cur = cur.sort(new BasicDBObject("_id", 1));
                        }

                        while(cur.hasNext()) {
                            DBObject doc = cur.next();
                            Map mydata;
                            mydata = doc.toMap();
                            mydata.put("_id", mydata.get("_id").toString());

                            // Should we ignore the first record _id == lastId?
                            //if (mydata.get("_id").toString().equals(lastId))
                            //    continue;

                            stream.add(mydata);
                        }
                    } catch (Exception e) {
                      if (closed) {
                          return;
                      }
                    }

                    // If the query doesn't return any results
                    // we should wait at least a second before
                    // doing the next query, Shouldn't we?
                    Thread.sleep(5000);
                    

                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

            }
        }
    }}
