/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.configuration;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class InferenceConfigParserTests extends AbstractXContentTestCase<InferenceConfigParser> {

    private String id = randomAlphaOfLength(10);

    @Override
    protected InferenceConfigParser doParseInstance(XContentParser parser) throws IOException {
        return InferenceConfigParser.parse(id, parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected InferenceConfigParser createTestInstance() {
        TaskType taskType = randomFrom(TaskType.values());
        String service = randomAlphaOfLength(10);
        Map<String, Object> serviceSettings = randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        return new InferenceConfigParser(id, taskType, service, serviceSettings);
    }

    public void testParseServiceSettings() throws IOException {
        var eisSparseSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettingsTests.createRandom();

        InferenceConfigParser parser = new InferenceConfigParser(
            id,
            TaskType.SPARSE_EMBEDDING,
            "elastic",
            XContentTestUtils.convertToMap(eisSparseSettings)
        );
        ElasticInferenceServiceSparseEmbeddingsServiceSettings parsedServiceSettings = parser.parseServiceSettings(
            ElasticInferenceServiceSparseEmbeddingsServiceSettings.class,
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(parsedServiceSettings, equalTo(eisSparseSettings));
    }
}
