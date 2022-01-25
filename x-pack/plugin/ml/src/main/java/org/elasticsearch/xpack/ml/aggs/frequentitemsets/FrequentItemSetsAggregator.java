/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: This isn't really a metric aggregation, however that's what scripted metric uses, too
 */
public class FrequentItemSetsAggregator extends MetricsAggregator {
    private static final Logger logger = LogManager.getLogger(FrequentItemSetsAggregator.class);

    interface TermValuesSource {
        String getString();
    }

    private final List<ValuesSourceConfig> configs;
    // private final List<TermValuesSource> values;

    private Map<String, Long> itemSetsDedupped = new HashMap<>();

    protected FrequentItemSetsAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        List<ValuesSourceConfig> configs
    ) throws IOException {
        super(name, context, parent, metadata);
        this.configs = configs;
        /*this.values = configs.stream()
            .map(c -> context.getValuesSourceRegistry().getAggregator(FrequentItemSetsAggregationBuilder.REGISTRY_KEY, c).build(c))
            .collect(Collectors.toList());*/
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        // TODO Auto-generated method stub
        return new InternalFrequentItemSetsAggregation(name, metadata(), itemSetsDedupped, null);
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE; // TODO: taken from scripted metric
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // TODO: very simplicitic version
        // List<String> items = items(ctx);

        return new LeafBucketCollectorBase(sub, configs) {

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                StringBuilder sb = new StringBuilder();
                // logger.info("build value");

                for (ValuesSourceConfig valueSourceConfig : configs) {
                    // logger.info("add value for [" + valueSourceConfig.getDescription() + "]");
                    ValuesSource valuesSource = valueSourceConfig.getValuesSource();
                    SortedBinaryDocValues values = valuesSource.bytesValues(ctx);

                    if (values.advanceExact(doc)) {
                        int valuesCount = values.docValueCount();
                        // logger.info("value count: [" + valuesCount + "]");

                        for (int i = 0; i < valuesCount; ++i) {
                            BytesRef bytes = values.nextValue();
                            sb.append(bytes.utf8ToString());
                            sb.append("#");
                        }

                    }
                }

                String key = sb.toString();
                // logger.info("add key: [" + key + "]");
                itemSetsDedupped.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
                // TODO: mapper implementation goes here
                // String key = Strings.collectionToCommaDelimitedString(items);
                // logger.info("collect [" + owningBucketOrd + "]");
                // logger.info("key: [" + key + "]");

                // itemSetsDedupped.compute(key, (k, v) -> (v == null) ? 1 : v + 1);

            }
        };
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFrequentItemSetsAggregation(name, metadata(), Collections.emptyMap(), null);
    }

    List<String> items(LeafReaderContext ctx) throws IOException {
        List<String> items = new ArrayList<>();
        // logger.info("collecting items");

        for (ValuesSourceConfig valueSourceConfig : configs) {
            ValuesSource valuesSource = valueSourceConfig.getValuesSource();

            if (valuesSource == null) {
                // logger.info("valuesSource == null");
                items.add(null);
            } else if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals source) {
                final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
                final long maxOrd = ordinalValues.getValueCount();
                if (maxOrd == 0) {
                    // logger.info("maxOrd == 0");

                    items.add(null);
                    continue;
                    // return Collections.emptyList();
                    // emptyCollectorsUsed++;
                    // return new EmptyCollector();
                }

                // final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
                // final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
                // only use ordinals if they don't increase memory usage by more than 25%
                // if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
                // ordinalsCollectorsUsed++;
                // return new OrdinalsCollector(counts, ordinalValues, bigArrays());
                // }
                // ordinalsCollectorsOverheadTooHigh++;
            }

            // MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
            StringBuilder sb = new StringBuilder();
            // logger.info("build value");

            SortedBinaryDocValues docValues = valuesSource.bytesValues(ctx);
            int valueCount = docValues.docValueCount();

            for (int i = 0; i < valueCount; ++i) {
                final BytesRef bytes = docValues.nextValue();
                // MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                sb.append(bytes.utf8ToString());
                sb.append("#");
            }
            items.add(sb.toString());
            // items.add(Long.toString(hash.h1));
        }
        return items;
    }

}
