/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.exponentialhistogram;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramEscfSchemaTests extends ESTestCase {

    /** The schema registers exactly the fields {@link ExponentialHistogramXContent} writes for a histogram with every part set. */
    public void testMatchesXContent() throws IOException {
        Map<String, Object> value = Map.of(
            "scale",
            3,
            "zero",
            Map.of("count", 2, "threshold", 0.5),
            "negative",
            Map.of("indices", List.of(-1, 0), "counts", List.of(1, 2)),
            "positive",
            Map.of("indices", List.of(4), "counts", List.of(3)),
            "sum",
            10.0,
            "min",
            -2.0,
            "max",
            7.0
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        ExponentialHistogramXContent.serialize(builder, ExponentialHistogramXContent.parseForTesting(value));
        Set<String> written = new HashSet<>();
        collectLeafPaths("histogram", XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(builder), false), written);

        SourceSchema schema = new SourceSchema();
        ExponentialHistogramEscfSchema histogram = schema.getSubSchema(0, "histogram", ExponentialHistogramEscfSchema.FACTORY);
        Set<String> registered = new HashSet<>();
        for (int leaf = 0; leaf < schema.leafCount(); leaf++) {
            registered.add(schema.getFullPath(leaf));
        }
        assertThat(registered, equalTo(written));

        assertThat(schema.getFullPath(histogram.scaleColIdx()), equalTo("histogram.scale"));
        assertThat(schema.getFullPath(histogram.zeroCountColIdx()), equalTo("histogram.zero.count"));
        assertThat(schema.getFullPath(histogram.zeroThresholdColIdx()), equalTo("histogram.zero.threshold"));
        assertThat(schema.getFullPath(histogram.negativeIndicesColIdx()), equalTo("histogram.negative.indices"));
        assertThat(schema.getFullPath(histogram.negativeCountsColIdx()), equalTo("histogram.negative.counts"));
        assertThat(schema.getFullPath(histogram.positiveIndicesColIdx()), equalTo("histogram.positive.indices"));
        assertThat(schema.getFullPath(histogram.positiveCountsColIdx()), equalTo("histogram.positive.counts"));
        assertThat(schema.getFullPath(histogram.sumColIdx()), equalTo("histogram.sum"));
        assertThat(schema.getFullPath(histogram.minColIdx()), equalTo("histogram.min"));
        assertThat(schema.getFullPath(histogram.maxColIdx()), equalTo("histogram.max"));
    }

    private static void collectLeafPaths(String path, Map<String, Object> object, Set<String> paths) {
        for (Map.Entry<String, Object> entry : object.entrySet()) {
            String childPath = path + "." + entry.getKey();
            if (entry.getValue() instanceof Map<?, ?> child) {
                @SuppressWarnings("unchecked")
                Map<String, Object> childObject = (Map<String, Object>) child;
                collectLeafPaths(childPath, childObject, paths);
            } else {
                paths.add(childPath);
            }
        }
    }
}
