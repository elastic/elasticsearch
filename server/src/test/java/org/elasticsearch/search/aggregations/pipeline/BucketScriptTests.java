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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BucketScriptTests extends BasePipelineAggregationTestCase<BucketScriptPipelineAggregationBuilder> {

    @Override
    protected BucketScriptPipelineAggregationBuilder createTestAggregatorFactory() {
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
            script = new Script(
                type,
                type == ScriptType.STORED ? null : randomFrom("my_lang", Script.DEFAULT_SCRIPT_LANG),
                "script",
                params
            );
        }
        BucketScriptPipelineAggregationBuilder factory = new BucketScriptPipelineAggregationBuilder(name, bucketsPaths, script);
        if (randomBoolean()) {
            factory.format(randomAlphaOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        return factory;
    }

    public void testParseBucketPath() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .field("buckets_path", "_count")
            .startObject("script")
            .field("source", "value")
            .field("lang", "expression")
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder builder1 = BucketScriptPipelineAggregationBuilder.PARSER.parse(
            createParser(content),
            "count"
        );
        assertEquals(builder1.getBucketsPaths().length, 1);
        assertEquals(builder1.getBucketsPaths()[0], "_count");

        content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("buckets_path")
            .field("path1", "_count1")
            .field("path2", "_count2")
            .endObject()
            .startObject("script")
            .field("source", "value")
            .field("lang", "expression")
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder builder2 = BucketScriptPipelineAggregationBuilder.PARSER.parse(
            createParser(content),
            "count"
        );
        assertEquals(builder2.getBucketsPaths().length, 2);
        assertEquals(builder2.getBucketsPaths()[0], "_count1");
        assertEquals(builder2.getBucketsPaths()[1], "_count2");

        content = XContentFactory.jsonBuilder()
            .startObject()
            .array("buckets_path", "_count1", "_count2")
            .startObject("script")
            .field("source", "value")
            .field("lang", "expression")
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder builder3 = BucketScriptPipelineAggregationBuilder.PARSER.parse(
            createParser(content),
            "count"
        );
        assertEquals(builder3.getBucketsPaths().length, 2);
        assertEquals(builder3.getBucketsPaths()[0], "_count1");
        assertEquals(builder3.getBucketsPaths()[1], "_count2");
    }

}
