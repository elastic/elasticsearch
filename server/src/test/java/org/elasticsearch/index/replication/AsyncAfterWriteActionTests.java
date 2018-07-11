/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.replication;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;


public class AsyncAfterWriteActionTests extends ESIndexLevelReplicationTestCase {

    public void testFailAsyncAfterWrite() throws Exception {
        final boolean failOnPrimary = randomBoolean();

        final AtomicInteger exceptionCount = new AtomicInteger();

        final IOException ioException = new IOException("xxx");
        final Function<ShardRouting, IOException> exceptionFunction = routing -> {
            if ((routing.primary() && failOnPrimary)
                || (routing.primary() == false && failOnPrimary == false)) {
                exceptionCount.incrementAndGet();
                return ioException;
            }
            return null;
        };
        try (ReplicationGroup shards = createGroup(0, exceptionFunction)) {
            shards.startAll();

            if (failOnPrimary == false) {
                IndexShard replica = shards.addReplica();
                shards.recoverReplica(replica);
            }

            final IndexRequest indexRequest = new IndexRequest(index.getName(), "type").source("{}", XContentType.JSON);
            indexRequest.onRetry(); // force an update of the timestamp
            final AssertionError assertionError = expectThrows(AssertionError.class, () -> shards.index(indexRequest));

            assertThat(exceptionCount.get(), equalTo(1));
            assertThat(assertionError.getCause(), equalTo(ioException));
        }
    }

    protected ReplicationGroup createGroup(int replicas, Function<ShardRouting, IOException> exceptionFunction) throws IOException {
        IndexMetaData metaData = buildIndexMetaData(replicas);
        ReplicationGroup shards = new ReplicationGroup(metaData) {
            @Override
            protected EngineFactory getEngineFactory(final ShardRouting routing) {
                return new InternalEngineFactory() {
                    @Override
                    public Engine newReadWriteEngine(EngineConfig config) {
                        return new InternalEngine(config) {
                            @Override
                            public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
                                final IOException exception = exceptionFunction.apply(routing);
                                if (exception != null) {
                                    throw exception;
                                }

                                return super.ensureTranslogSynced(locations);
                            }
                        };
                    }
                };
            }
        };

        return shards;
    }
}
