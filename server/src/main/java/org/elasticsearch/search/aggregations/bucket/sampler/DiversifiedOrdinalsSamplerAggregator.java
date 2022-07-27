/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.misc.search.DiversifiedTopDocsCollector;
import org.apache.lucene.search.TopDocsCollector;
import org.elasticsearch.index.fielddata.AbstractNumericDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.lucene.misc.search.DiversifiedTopDocsCollector.ScoreDocKey;

public class DiversifiedOrdinalsSamplerAggregator extends SamplerAggregator {

    private ValuesSource.Bytes.WithOrdinals.FieldData valuesSource;
    private int maxDocsPerValue;

    DiversifiedOrdinalsSamplerAggregator(
        String name,
        int shardSize,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        ValuesSourceConfig valuesSourceConfig,
        int maxDocsPerValue
    ) throws IOException {
        super(name, shardSize, factories, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSourceConfig.getValuesSource();
        this.maxDocsPerValue = maxDocsPerValue;
    }

    @Override
    public DeferringBucketCollector buildDeferringCollector() {
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
            super(shardSize, bigArrays(), circuitBreakerConsumer);
        }

        @Override
        protected TopDocsCollector<ScoreDocKey> createTopDocsCollector(int size) {
            // Make sure we do not allow size > maxDoc, to prevent accidental OOM
            int minMaxDocsPerValue = Math.min(maxDocsPerValue, searcher().getIndexReader().maxDoc());
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
                                throw new IllegalArgumentException("Sample diversifying key must be a single valued-field");
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
