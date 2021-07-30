/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatafeedUpdateTests extends AbstractXContentTestCase<DatafeedUpdate> {

    public static DatafeedUpdate createRandom() {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder(DatafeedConfigTests.randomValidDatafeedId());
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        if (randomBoolean()) {
            builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        if (randomBoolean()) {
            builder.setIndices(DatafeedConfigTests.randomStringList(1, 10));
        }
        if (randomBoolean()) {
            try {
                builder.setQuery(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize query", e);
            }
        }
        if (randomBoolean()) {
            int scriptsSize = randomInt(3);
            List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
            for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                scriptFields.add(new SearchSourceBuilder.ScriptField(randomAlphaOfLength(10), mockScript(randomAlphaOfLength(10)),
                    randomBoolean()));
            }
            builder.setScriptFields(scriptFields);
        }
        if (randomBoolean()) {
            // can only test with a single agg as the xcontent order gets randomized by test base class and then
            // the actual xcontent isn't the same and test fail.
            // Testing with a single agg is ok as we don't have special list xcontent logic
            AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
            aggs.addAggregator(AggregationBuilders.avg(randomAlphaOfLength(10)).field(randomAlphaOfLength(10)));
            try {
                builder.setAggregations(aggs);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize aggs", e);
            }
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        if (randomBoolean()) {
            builder.setDelayedDataCheckConfig(DelayedDataCheckConfigTests.createRandomizedConfig());
        }
        if (randomBoolean()) {
            builder.setMaxEmptySearches(randomIntBetween(10, 100));
        }
        if (randomBoolean()) {
            builder.setIndicesOptions(IndicesOptions.fromOptions(randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()));
        }
        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("type", "keyword");
            settings.put("script", "");
            Map<String, Object> field = new HashMap<>();
            field.put("runtime_field_foo", settings);
            builder.setRuntimeMappings(field);
        }
        return builder.build();
    }

    @Override
    protected DatafeedUpdate createTestInstance() {
        return createRandom();
    }

    @Override
    protected DatafeedUpdate doParseInstance(XContentParser parser) {
        return DatafeedUpdate.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
