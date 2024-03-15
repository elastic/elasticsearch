/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SimulateBulkRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        testSerialization(getTestPipelineSubstitutions());
        testSerialization(null);
        testSerialization(Map.of());
    }

    private void testSerialization(Map<String, Map<String, Object>> pipelineSubstitutions) throws IOException {
        SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest(pipelineSubstitutions);
        /*
         * Note: SimulateBulkRequest does not implement equals or hashCode, so we can't test serialization in the usual way for a
         * Writable
         */
        SimulateBulkRequest copy = copyWriteable(simulateBulkRequest, null, SimulateBulkRequest::new);
        assertThat(copy.getPipelineSubstitutions(), equalTo(simulateBulkRequest.getPipelineSubstitutions()));
    }

    private static Map<String, Map<String, Object>> getTestPipelineSubstitutions() {
        return Map.of(
            "pipeline1",
            Map.of("processors", List.of(Map.of("processor2", Map.of()), Map.of("processor3", Map.of()))),
            "pipeline2",
            Map.of("processors", List.of(Map.of("processor3", Map.of())))
        );
    }
}
