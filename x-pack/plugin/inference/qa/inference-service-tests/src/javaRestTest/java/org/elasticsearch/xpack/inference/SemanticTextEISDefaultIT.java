/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_EIS_ELSER_INFERENCE_ID;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end test that verifies semantic_text fields automatically default to ELSER on EIS
 * when available and no inference_id is explicitly provided.
 */
public class SemanticTextEISDefaultIT extends BaseMockEISAuthServerTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Ensure the mock EIS server has an authorized response ready before each test
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    /**
     * This is done before the class because I've run into issues where another class that extends {@link BaseMockEISAuthServerTest}
     * results in an authorization response not being queued up for the new Elasticsearch Node in time. When the node starts up, it
     * retrieves authorization. If the request isn't queued up when that happens the tests will fail. From my testing locally it seems
     * like the base class's static functionality to queue a response is only done once and not for each subclass.
     *
     * My understanding is that the @Before will be run after the node starts up and wouldn't be sufficient to handle
     * this scenario. That is why this needs to be @BeforeClass.
     */
    @BeforeClass
    public static void init() {
        // Ensure the mock EIS server has an authorized response ready
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    public void testDefaultInferenceIdForSemanticText() throws IOException {
        String indexName = "semantic-index";
        String mapping = """
            {
              "properties": {
                "semantic_text_field": {
                  "type": "semantic_text"
                }
              }
            }
            """;
        Settings settings = Settings.builder().build();
        createIndex(indexName, settings, mapping);

        Map<String, Object> mappingAsMap = getIndexMappingAsMap(indexName);
        String populatedInferenceId = (String) XContentMapValues.extractValue("properties.semantic_text_field.inference_id", mappingAsMap);

        assertThat(
            "semantic_text field should default to ELSER on EIS when available",
            populatedInferenceId,
            equalTo(DEFAULT_EIS_ELSER_INFERENCE_ID)
        );
    }

}
