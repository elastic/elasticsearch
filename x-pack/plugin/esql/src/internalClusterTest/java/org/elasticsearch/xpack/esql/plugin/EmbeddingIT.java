/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the ESQL COMPLETION command.
 * Tests cover basic functionality, custom field names, filtering, concatenated prompts, and settings.
 */
public class EmbeddingIT extends InferenceCommandIntegTestCase {

    private static final String TEST_INDEX = "test_embedding";
    private static final String EMBEDDING_MODEL_ID = "test-embedding-model";

    @Before
    public void setupIndexAndInferenceModel() throws IOException {

        String config = String.format(Locale.ROOT, """
            {
              "service": "%s",
              "service_settings": {
                "model_id": "test-%s",
                "api_key": "test-key",
                "dimensions": 3
              }
            }
            """, "text_embedding_test_service", TaskType.EMBEDDING.toString().toLowerCase(Locale.ROOT));

        createAndPopulateTestIndex(TEST_INDEX);

        client().execute(
            PutInferenceModelAction.INSTANCE,
            new PutInferenceModelAction.Request(TaskType.EMBEDDING, EMBEDDING_MODEL_ID, new BytesArray(config), XContentType.JSON, TEST_REQUEST_TIMEOUT)
        ).actionGet();
    }

    @After
    public void cleanup() {
        deleteTestInferenceEndpoint(EMBEDDING_MODEL_ID, TaskType.EMBEDDING);
    }

    public void testEmbedding() {
        var query = String.format(Locale.ROOT, """
            ROW text = "my embedding test"
            | EVAL s = EMBEDDING(text, "%s")
            """, EMBEDDING_MODEL_ID);

        var resp = run(query);
        assertNotNull(resp);
    }
}


