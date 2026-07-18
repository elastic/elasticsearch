/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.usage.ModelStats;
import org.elasticsearch.xpack.core.inference.usage.ModelStatsTests;
import org.elasticsearch.xpack.core.ml.stats.SizeHistogramAccumulator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class InferenceFeatureSetUsageTests extends AbstractWireSerializingTestCase<InferenceFeatureSetUsage> {

    public void testXContentIncludesConfigSizes() throws IOException {
        SizeHistogramAccumulator inferenceId = new SizeHistogramAccumulator();
        inferenceId.add(12);
        InferenceFeatureSetUsage usage = new InferenceFeatureSetUsage(List.of(), Map.of("inference_id", inferenceId.asMap()));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> source = XContentHelper.convertToMap(BytesReference.bytes(builder), true, XContentType.JSON).v2();

        assertThat(source.containsKey(InferenceFeatureSetUsage.CONFIG_SIZES_FIELD), is(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> configSizes = (Map<String, Object>) source.get(InferenceFeatureSetUsage.CONFIG_SIZES_FIELD);
        assertThat(configSizes.containsKey("inference_id"), is(true));
    }

    @Override
    protected Writeable.Reader<InferenceFeatureSetUsage> instanceReader() {
        return InferenceFeatureSetUsage::new;
    }

    @Override
    protected InferenceFeatureSetUsage createTestInstance() {
        return new InferenceFeatureSetUsage(randomList(10, ModelStatsTests::createRandomInstance));
    }

    @Override
    protected InferenceFeatureSetUsage mutateInstance(InferenceFeatureSetUsage instance) throws IOException {
        List<ModelStats> mutatedModelStats = new ArrayList<>(instance.modelStats());
        if (mutatedModelStats.isEmpty()) {
            mutatedModelStats.add(ModelStatsTests.createRandomInstance());
        } else {
            mutatedModelStats.remove(randomIntBetween(0, mutatedModelStats.size() - 1));
        }
        return new InferenceFeatureSetUsage(mutatedModelStats);
    }
}
