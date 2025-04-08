/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

public class SemanticTextUpgradeIT extends AbstractUpgradeTestCase {
    private static final String INDEX_BASE_NAME = "semantic_text_test_index";
    private static final String SEMANTIC_TEXT_FIELD = "semantic_field";
    private static final String INFERENCE_ID = "dummy_id";

    private final boolean useLegacyFormat;

    public SemanticTextUpgradeIT(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    public void testSemanticTextOperations() throws Exception {
        switch (CLUSTER_TYPE) {
            case OLD -> createAndPopulateIndex();
        }
    }

    private void createAndPopulateIndex() throws IOException {
        final String mapping = """
            {
              "properties": {
                "%s": {
                  "type": "semantic_text",
                  "inference_id": "%s"
                }
              }
            }
            """.formatted(SEMANTIC_TEXT_FIELD, INFERENCE_ID);

        CreateIndexResponse response = createIndex(
            getIndexName(),
            Settings.builder().put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat).build(),
            mapping
        );
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    private String getIndexName() {
        return INDEX_BASE_NAME + (useLegacyFormat ? "_legacy" : "_new");
    }
}
