/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationResponseEntityTests extends ESTestCase {

    public void testParseAllFields() throws IOException {
        String json = """
                {
                    "models": [
                        {
                            "model_name": "test_model",
                            "task_types": ["embed/text/sparse", "chat"]
                        }
                    ]
                }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var entity = ElasticInferenceServiceAuthorizationResponseEntity.PARSER.apply(parser, null);
            var expected = new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "test_model",
                        EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)
                    )
                )
            );

            assertThat(entity, is(expected));
        }
    }

    public void testParsing_EmptyModels() throws IOException {
        String json = """
                {
                    "models": []
                }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var entity = ElasticInferenceServiceAuthorizationResponseEntity.PARSER.apply(parser, null);
            var expected = new ElasticInferenceServiceAuthorizationResponseEntity(List.of());

            assertThat(entity, is(expected));
        }
    }

}
