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

package org.elasticsearch.indices.ttl;

import org.apache.lucene.document.Document;

import org.apache.lucene.index.IndexReader;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticSearchException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.selector.UidFieldSelector;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * A node level service that delete expired docs on node primary shards.
 *
 */
public class IndicesTTLService extends AbstractLifecycleComponent<IndicesTTLService> {

    private static final String SETTING_PURGE_INTERVAL = "purge_interval";
    private static final TimeValue DEFAULT_PURGE_INTERVAL = new TimeValue(60, TimeUnit.SECONDS);
    private static final String SETTINGS_BULK_SIZE = "bulk_size";
    private static final int DEFAULT_BULK_SIZE = 10000;

    private final IndicesService indicesService;
    private final Client client;

    private final TimeValue purgeInterval;
    private final int bulkSize;
    private BulkRequestBuilder bulkRequest;
    private PurgerThread purgerThread;

    @Inject public IndicesTTLService(Settings settings, IndicesService indicesService, Client client) {
        super(settings);
        this.indicesService = indicesService;
        this.client = client;
        this.purgeInterval = componentSettings.getAsTime(SETTING_PURGE_INTERVAL, DEFAULT_PURGE_INTERVAL);
        this.bulkSize = componentSettings.getAsInt(SETTINGS_BULK_SIZE, DEFAULT_BULK_SIZE);
    }

    @Override protected void doStart() throws ElasticSearchException {
        this.purgerThread = new PurgerThread(EsExecutors.threadName(settings, "[purger]"));
        this.purgerThread.start();
    }

    @Override protected void doStop() throws ElasticSearchException {
        this.purgerThread.doStop();
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    private class PurgerThread extends Thread {
        volatile boolean running = true;

        public PurgerThread(String name) {
            super(name);
            setDaemon(true);
        }

        public void doStop() {
            running = false;
        }

        public void run() {
            while (running) {
                List<IndexShard> shardsToPurge = getShardsToPurge();
                purgeShards(shardsToPurge);
                try {
                    Thread.sleep(purgeInterval.millis());
                } catch (InterruptedException e) {
                    running = false;
                    return;
                }

            }
        }

        /**
         * Returns the shards to purge, i.e. the local started primary shards that have ttl enabled
         */
        private List<IndexShard> getShardsToPurge() {
            List<IndexShard> shardsToPurge = new ArrayList<IndexShard>();
            for (IndexService indexService : indicesService) {
                // should be optimized with the hasTTL flag
                FieldMappers ttlFieldMappers = indexService.mapperService().name(TTLFieldMapper.NAME);
                // check if ttl is enabled for at least one type of this index
                boolean hasTTLEnabled = false;
                for (FieldMapper ttlFieldMapper : ttlFieldMappers) {
                    if (((TTLFieldMapper)ttlFieldMapper).enabled()) {
                        hasTTLEnabled = true;
                        break;
                    }
                }
                if (hasTTLEnabled)
                {
                    for (Integer shardId : indexService.shardIds()) {
                        IndexShard shard = indexService.shard(shardId);
                        if (shard.routingEntry().primary() && shard.state() == IndexShardState.STARTED && shard.routingEntry().started()) {
                            shardsToPurge.add(shard);
                        }
                    }
                }
            }
            return shardsToPurge;
        }
    }

    private void purgeShards(List<IndexShard> shardsToPurge) {
        for (IndexShard shardToPurge : shardsToPurge) {
            Query query = NumericRangeQuery.newLongRange(TTLFieldMapper.NAME, null, System.currentTimeMillis(), false, true);
            Engine.Searcher searcher = shardToPurge.searcher();
            try {
                logger.debug("[{}][{}] purging shard", shardToPurge.routingEntry().index(), shardToPurge.routingEntry().id());
                ExpiredDocsCollector expiredDocsCollector = new ExpiredDocsCollector();
                searcher.searcher().search(query, expiredDocsCollector);
                List<DocToPurge> docsToPurge = expiredDocsCollector.getDocsToPurge();
                bulkRequest = client.prepareBulk();
                for (DocToPurge docToPurge : docsToPurge) {
                    bulkRequest.add(new DeleteRequest().index(shardToPurge.routingEntry().index()).type(docToPurge.type).id(docToPurge.id).version(docToPurge.version));
                    processBulkIfNeeded(false);
                }
                processBulkIfNeeded(true);
            } catch (Exception e) {
                logger.warn("failed to purge", e);
            } finally {
                searcher.release();
            }
        }
    }

    private static class DocToPurge {
        public final String type;
        public final String id;
        public final long version;

        public DocToPurge(String type, String id, long version) {
            this.type = type;
            this.id = id;
            this.version = version;
        }
    }

    private class ExpiredDocsCollector extends Collector {
        private IndexReader indexReader;
        private List<DocToPurge> docsToPurge = new ArrayList<DocToPurge>();

        public ExpiredDocsCollector() {
        }

        public void setScorer(Scorer scorer) {
        }

        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        public void collect(int doc) {
            try {
                Document document = indexReader.document(doc, UidFieldSelector.INSTANCE);
                String uid = document.getFieldable(UidFieldMapper.NAME).stringValue();
                long version = UidField.loadVersion(indexReader, UidFieldMapper.TERM_FACTORY.createTerm(uid));
                docsToPurge.add(new DocToPurge(Uid.typeFromUid(uid),Uid.idFromUid(uid), version));
            } catch (Exception e) {
            }
        }

        public void setNextReader(IndexReader reader, int docBase) {
            this.indexReader = reader;
        }

        public List<DocToPurge> getDocsToPurge() {
            return this.docsToPurge;
        }
    }

    private void processBulkIfNeeded(boolean force) {
        if ((force && bulkRequest.numberOfActions() > 0) || bulkRequest.numberOfActions() >= bulkSize) {
            try {
                bulkRequest.execute(new ActionListener<BulkResponse>() {
                    @Override public void onResponse(BulkResponse bulkResponse) {
                        logger.debug("bulk took " + bulkResponse.getTookInMillis() + "ms");
                    }

                    @Override public void onFailure(Throwable e) {
                        logger.warn("failed to execute bulk");
                    }
                });
            } catch (Exception e) {
                logger.warn("failed to process bulk", e);
            }
            bulkRequest = client.prepareBulk();
        }
    }
}