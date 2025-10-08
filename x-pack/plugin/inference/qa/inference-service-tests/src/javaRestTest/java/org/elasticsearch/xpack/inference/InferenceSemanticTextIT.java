/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;

import java.io.IOException;

import java.util.Map;



import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.DEFAULT_ELSER_ENDPOINT_ID_V2;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.DEFAULT_ELSER_ID;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Simple end-to-end test that verifies Elasticsearch automatically assigns the
 * default ELSER inference endpoint ID to a {@code semantic_text} field mapping
 * when none is provided by the user.
 */
public class InferenceSemanticTextIT extends BaseMockEISAuthServerTest {

    @Before
    public void setUpMockServer() throws Exception {
        super.setUp();
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    public void testDefaultInferenceIdForSemanticText() throws IOException {
        String indexName = "semantic-index";
        String mapping = """
            {
              \"properties\": {
                \"semantic_text_field\": {
                  \"type\": \"semantic_text\"
                }
              }
            }
            """;
        Settings settings = Settings.builder().build();
        createIndex(indexName, settings, mapping);

        Map<String, Object> mappingAsMap = getIndexMappingAsMap(indexName);
        ObjectPath path = new ObjectPath(mappingAsMap);
        String populatedInferenceId = path.evaluate("properties.semantic_text_field.inference_id");
        assertThat("[inference_id] should be auto-populated", populatedInferenceId, notNullValue());

        assertThat(
            populatedInferenceId,
            anyOf(equalTo(DEFAULT_ELSER_ID), equalTo(DEFAULT_ELSER_ENDPOINT_ID_V2))
        );


    }

}
