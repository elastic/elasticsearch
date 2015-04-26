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
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DiversifiedTopDocsCollector;
import org.apache.lucene.search.DiversifiedTopDocsCollector.ScoreDocKey;
import org.apache.lucene.search.TopDocsCollector;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BestDocsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

public class DiversifiedOrdinalsSamplerAggregator extends SamplerAggregator {

    private ValuesSource.Bytes.WithOrdinals.FieldData valuesSource;
    private int maxDocsPerValue;

    public DiversifiedOrdinalsSamplerAggregator(String name, int shardSize, AggregatorFactories factories,
            AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData,
            ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, int maxDocsPerValue) throws IOException {
        super(name, shardSize, factories, aggregationContext, parent, metaData);
        this.valuesSource = valuesSource;
        this.maxDocsPerValue = maxDocsPerValue;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        bdd = new DiverseDocsDeferringCollector();
        return bdd;
    }

    /**
     * A {@link DeferringBucketCollector} that identifies top scoring documents
     * but de-duped by a key then passes only these on to nested collectors.
     * This implementation is only for use with a single bucket aggregation.
     */
    class DiverseDocsDeferringCollector extends BestDocsDeferringCollector {

        public DiverseDocsDeferringCollector() {
            super(shardSize);
        }

        @Override
        protected TopDocsCollector<ScoreDocKey> createTopDocsCollector(int size) {
            return new ValuesDiversifiedTopDocsCollector(size, maxDocsPerValue);
        }

        // This class extends the DiversifiedTopDocsCollector and provides
        // a lookup from elasticsearch's ValuesSource
        class ValuesDiversifiedTopDocsCollector extends DiversifiedTopDocsCollector {


            public ValuesDiversifiedTopDocsCollector(int numHits, int maxHitsPerKey) {
                super(numHits, maxHitsPerKey);
            }

            @Override
            protected NumericDocValues getKeys(LeafReaderContext context) {
                final RandomAccessOrds globalOrds = valuesSource.globalOrdinalsValues(context);
                final SortedDocValues singleValues = DocValues.unwrapSingleton(globalOrds);
                if (singleValues != null) {
                    return new NumericDocValues() {
                        @Override
                        public long get(int doc) {
                            return singleValues.getOrd(doc);
                        }
                    };
                }
                return new NumericDocValues() {
                    @Override
                    public long get(int doc) {
                        globalOrds.setDocument(doc);
                        final long valuesCount = globalOrds.cardinality();
                        if (valuesCount > 1) {
                            throw new ElasticsearchIllegalArgumentException("Sample diversifying key must be a single valued-field");
                        }
                        if (valuesCount == 1) {
                            long result = globalOrds.ordAt(0);
                            return result;
                        }
                        return Long.MIN_VALUE;
                    }
                };

            }

        }

    }

}
