/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;

public class BucketSelectorTests extends BasePipelineAggregationTestCase<BucketSelectorPipelineAggregationBuilder> {

    @Override
    protected BucketSelectorPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        Map<String, String> bucketsPaths = new HashMap<>();
        int numBucketPaths = randomIntBetween(1, 10);
        for (int i = 0; i < numBucketPaths; i++) {
            bucketsPaths.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 40));
        }
        Script script;
        if (randomBoolean()) {
            script = mockScript("script");
        } else {
            Map<String, Object> params = new HashMap<>();
            if (randomBoolean()) {
                params.put("foo", "bar");
            }
            ScriptType type = randomFrom(ScriptType.values());
            script =
                new Script(type, type == ScriptType.STORED ? null : randomFrom("my_lang", Script.DEFAULT_SCRIPT_LANG), "script", params);
        }
        BucketSelectorPipelineAggregationBuilder factory = new BucketSelectorPipelineAggregationBuilder(name, bucketsPaths, script);
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        return factory;
    }

    public void testNoParent() {
        assertThat(validate(emptyList(), new BucketSelectorPipelineAggregationBuilder("foo", emptyMap(), new Script("foo"))),
            equalTo("Validation Failed: 1: bucket_selector aggregation [foo] must be declared inside of another aggregation;"));
    }
}
