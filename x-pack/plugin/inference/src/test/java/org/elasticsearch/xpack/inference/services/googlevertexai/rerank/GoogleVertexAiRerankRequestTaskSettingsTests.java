/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GoogleVertexAiRerankRequestTaskSettingsTests extends ESTestCase {

    public void testFromMap_ReturnsEmptySettings_IfMapEmpty() {
        var requestTaskSettings = GoogleVertexAiRerankRequestTaskSettings.fromMap(new HashMap<>());
        assertThat(requestTaskSettings, is(GoogleVertexAiRerankRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_DoesNotThrowValidationException_IfTopNIsMissing() {
        var requestTaskSettings = GoogleVertexAiRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of("unrelated", 1)));
        assertThat(requestTaskSettings, is(new GoogleVertexAiRerankRequestTaskSettings(null)));
    }

    public void testFromMap_ExtractsTopN() {
        var topN = 1;
        var requestTaskSettings = GoogleVertexAiRerankRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(GoogleVertexAiRerankTaskSettings.TOP_N, topN))
        );
        assertThat(requestTaskSettings, is(new GoogleVertexAiRerankRequestTaskSettings(topN)));
    }
}
