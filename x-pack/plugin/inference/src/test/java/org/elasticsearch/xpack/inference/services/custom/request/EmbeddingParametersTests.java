/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.services.custom.InputTypeTranslator;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class EmbeddingParametersTests extends ESTestCase {

    public void testTaskTypeParameters_UsesDefaultValue() {
        var parameters = EmbeddingParameters.of(
            new EmbeddingsInput(List.of("input"), null, InputType.INGEST),
            new InputTypeTranslator(Map.of(), "default")
        );

        assertThat(parameters.taskTypeParameters(), is(Map.of("input_type", "\"default\"")));
    }

    public void testTaskTypeParameters_UsesMappedValue() {
        var parameters = EmbeddingParameters.of(
            new EmbeddingsInput(List.of("input"), null, InputType.INGEST),
            new InputTypeTranslator(Map.of(InputType.INGEST, "ingest_value"), "default")
        );

        assertThat(parameters.taskTypeParameters(), is(Map.of("input_type", "\"ingest_value\"")));
    }
}
