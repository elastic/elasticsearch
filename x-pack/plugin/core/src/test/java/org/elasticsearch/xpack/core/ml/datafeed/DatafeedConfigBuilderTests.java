/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests.randomStringList;
import static org.elasticsearch.xpack.core.ml.utils.QueryProviderTests.createRandomValidQueryProvider;

public class DatafeedConfigBuilderTests extends AbstractWireSerializingTestCase<DatafeedConfig.Builder> {

    public static DatafeedConfig.Builder createRandomizedDatafeedConfigBuilder(String jobId, String datafeedId, long bucketSpanMillis) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder();
        if (jobId != null) {
            builder.setJobId(jobId);
        }
        if (datafeedId != null) {
            builder.setId(datafeedId);
        }
        builder.setIndices(randomStringList(1, 10));
        if (randomBoolean()) {
            builder.setQueryProvider(createRandomValidQueryProvider(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
        }
        boolean addScriptFields = randomBoolean();
        if (addScriptFields) {
            int scriptsSize = randomInt(3);
            List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
            for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                scriptFields.add(new SearchSourceBuilder.ScriptField(randomAlphaOfLength(10), mockScript(randomAlphaOfLength(10)),
                    randomBoolean()));
            }
            builder.setScriptFields(scriptFields);
        }
        Long aggHistogramInterval = null;
        if (randomBoolean() && addScriptFields == false) {
            // can only test with a single agg as the xcontent order gets randomized by test base class and then
            // the actual xcontent isn't the same and test fail.
            // Testing with a single agg is ok as we don't have special list writeable / xcontent logic
            AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
            aggHistogramInterval = randomNonNegativeLong();
            aggHistogramInterval = aggHistogramInterval> bucketSpanMillis ? bucketSpanMillis : aggHistogramInterval;
            aggHistogramInterval = aggHistogramInterval <= 0 ? 1 : aggHistogramInterval;
            MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
            AggregationBuilder topAgg = randomBoolean() ?
                AggregationBuilders.dateHistogram("buckets")
                    .field("time")
                    .fixedInterval(new DateHistogramInterval(aggHistogramInterval + "ms")) :
                AggregationBuilders.composite(
                    "buckets",
                    Collections.singletonList(
                        new DateHistogramValuesSourceBuilder("time")
                            .field("time")
                            .fixedInterval(new DateHistogramInterval(aggHistogramInterval + "ms"))
                    )
                );
            aggs.addAggregator(topAgg.subAggregation(maxTime));
            builder.setParsedAggregations(aggs);
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            if (aggHistogramInterval == null) {
                builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
            } else {
                builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, 5) * aggHistogramInterval));
            }
        }
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        if (randomBoolean()) {
            builder.setDelayedDataCheckConfig(DelayedDataCheckConfigTests.createRandomizedConfig(bucketSpanMillis));
        }
        if (randomBoolean()) {
            builder.setMaxEmptySearches(randomIntBetween(10, 100));
        }
        builder.setIndicesOptions(IndicesOptions.fromParameters(
            randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
            Boolean.toString(randomBoolean()),
            Boolean.toString(randomBoolean()),
            Boolean.toString(randomBoolean()),
            SearchRequest.DEFAULT_INDICES_OPTIONS));
        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("type", "keyword");
            settings.put("script", "");
            Map<String, Object> field = new HashMap<>();
            field.put("runtime_field_foo", settings);
            builder.setRuntimeMappings(field);
        }
        return builder;
    }

    @Override
    protected DatafeedConfig.Builder createTestInstance() {
        return createRandomizedDatafeedConfigBuilder(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            3600000
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<DatafeedConfig.Builder> instanceReader() {
        return DatafeedConfig.Builder::new;
    }

}
