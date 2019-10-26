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

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DiversifiedTopDocsCollector;
import org.apache.lucene.search.DiversifiedTopDocsCollector.ScoreDocKey;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.AbstractNumericDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DiversifiedMapSamplerAggregator extends SamplerAggregator {

    private ValuesSource valuesSource;
    private int maxDocsPerValue;
    private BytesRefHash bucketOrds;

    DiversifiedMapSamplerAggregator(String name, int shardSize, AggregatorFactories factories,
            SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
            ValuesSource valuesSource, int maxDocsPerValue) throws IOException {
        super(name, shardSize, factories, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.maxDocsPerValue = maxDocsPerValue;
        // Need to use super class shardSize since it is limited to maxDoc
        bucketOrds = new BytesRefHash(this.shardSize, context.bigArrays());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
        super.doClose();
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        bdd = new DiverseDocsDeferringCollector(this::addRequestCircuitBreakerBytes);
        return bdd;
    }

    /**
     * A {@link DeferringBucketCollector} that identifies top scoring documents
     * but de-duped by a key then passes only these on to nested collectors.
     * This implementation is only for use with a single bucket aggregation.
     */
    class DiverseDocsDeferringCollector extends BestDocsDeferringCollector {

        DiverseDocsDeferringCollector(Consumer<Long> circuitBreakerConsumer) {
            super(shardSize, context.bigArrays(), circuitBreakerConsumer);
        }

        @Override
        protected TopDocsCollector<ScoreDocKey> createTopDocsCollector(int size) {
            // Make sure we do not allow size > maxDoc, to prevent accidental OOM
            int minMaxDocsPerValue = Math.min(maxDocsPerValue, context.searcher().getIndexReader().maxDoc());
            return new ValuesDiversifiedTopDocsCollector(size, minMaxDocsPerValue);
        }

        @Override
        protected long getPriorityQueueSlotSize() {
            return SCOREDOCKEY_SIZE;
        }

        // This class extends the DiversifiedTopDocsCollector and provides
        // a lookup from elasticsearch's ValuesSource
        class ValuesDiversifiedTopDocsCollector extends DiversifiedTopDocsCollector {

            private SortedBinaryDocValues values;

            ValuesDiversifiedTopDocsCollector(int numHits, int maxHitsPerKey) {
                super(numHits, maxHitsPerKey);
            }

            @Override
            protected NumericDocValues getKeys(LeafReaderContext context) {
                try {
                    values = valuesSource.bytesValues(context);
                } catch (IOException e) {
                    throw new ElasticsearchException("Error reading values", e);
                }
                return new AbstractNumericDocValues() {

                    private int docID = -1;

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        docID = target;
                        if (values.advanceExact(target)) {
                            if (values.docValueCount() > 1) {
                                throw new IllegalArgumentException(
                                        "Sample diversifying key must be a single valued-field");
                            }
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public int docID() {
                        return docID;
                    }

                    @Override
                    public long longValue() throws IOException {
                        final BytesRef bytes = values.nextValue();

                        long bucketOrdinal = bucketOrds.add(bytes);
                        if (bucketOrdinal < 0) { // already seen
                            bucketOrdinal = -1 - bucketOrdinal;
                        }
                        return bucketOrdinal;
                    }
                };
            }

        }

    }

}
