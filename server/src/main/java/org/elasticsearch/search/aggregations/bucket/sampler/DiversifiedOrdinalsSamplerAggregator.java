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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DiversifiedTopDocsCollector;
import org.apache.lucene.search.DiversifiedTopDocsCollector.ScoreDocKey;
import org.apache.lucene.search.TopDocsCollector;
import org.elasticsearch.index.fielddata.AbstractNumericDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public class DiversifiedOrdinalsSamplerAggregator extends SamplerAggregator {

    private ValuesSource.Bytes.WithOrdinals.FieldData valuesSource;
    private int maxDocsPerValue;

    DiversifiedOrdinalsSamplerAggregator(String name, int shardSize, AggregatorFactories factories,
            SearchContext context, Aggregator parent, Map<String, Object> metadata,
            ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, int maxDocsPerValue) throws IOException {
        super(name, shardSize, factories, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.maxDocsPerValue = maxDocsPerValue;
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


            ValuesDiversifiedTopDocsCollector(int numHits, int maxHitsPerKey) {
                super(numHits, maxHitsPerKey);
            }

            @Override
            protected NumericDocValues getKeys(LeafReaderContext context) {
                final SortedSetDocValues globalOrds = valuesSource.globalOrdinalsValues(context);
                final SortedDocValues singleValues = DocValues.unwrapSingleton(globalOrds);
                if (singleValues != null) {
                    return new AbstractNumericDocValues() {

                        @Override
                        public boolean advanceExact(int target) throws IOException {
                            return singleValues.advanceExact(target);
                        }

                        @Override
                        public int docID() {
                            return singleValues.docID();
                        }

                        @Override
                        public long longValue() throws IOException {
                            return singleValues.ordValue();
                        }
                    };
                }
                return new AbstractNumericDocValues() {

                    long value;

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        if (globalOrds.advanceExact(target)) {
                            value = globalOrds.nextOrd();
                            // Check there isn't a second value for this
                            // document
                            if (globalOrds.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
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
                        return globalOrds.docID();
                    }

                    @Override
                    public long longValue() throws IOException {
                        return value;
                    }
                };

            }

        }

    }

}
