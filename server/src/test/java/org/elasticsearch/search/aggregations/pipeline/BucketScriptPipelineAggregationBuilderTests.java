/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;

public class BucketScriptPipelineAggregationBuilderTests extends BasePipelineAggregationTestCase<BucketScriptPipelineAggregationBuilder> {
    @Override
    protected BucketScriptPipelineAggregationBuilder createTestAggregatorFactory() {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        int targetBucketsPathsMapSize = randomInt(5);
        while (bucketsPathsMap.size() < targetBucketsPathsMapSize) {
            bucketsPathsMap.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        Script script = new Script(randomAlphaOfLength(4));
        return new BucketScriptPipelineAggregationBuilder(randomAlphaOfLength(3), bucketsPathsMap, script);
    }

    public void testNoParent() {
        assertThat(validate(emptyList(), new BucketScriptPipelineAggregationBuilder("foo", emptyMap(), new Script("foo"))),
            equalTo("Validation Failed: 1: bucket_script aggregation [foo] must be declared inside of another aggregation;"));
    }
}
