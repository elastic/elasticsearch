/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_EIS_ELSER_INFERENCE_ID;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end test that verifies semantic_text fields automatically default to ELSER on EIS
 * when available and no inference_id is explicitly provided.
 */
public class SemanticTextEISDefaultIT extends BaseMockEISAuthServerTest {

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
