/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class RerankParametersTests extends ESTestCase {

    public void testTaskTypeParameters() {
        var queryAndDocsInputs = new QueryAndDocsInputs("query_value", List.of("doc1", "doc2"), true, 5, false);
        var parameters = RerankParameters.of(queryAndDocsInputs);

        assertThat(parameters.taskTypeParameters(), is(Map.of("query", "\"query_value\"", "top_n", "5", "return_documents", "true")));
    }

    public void testTaskTypeParameters_WithoutOptionalFields() {
        var queryAndDocsInputs = new QueryAndDocsInputs("query_value", List.of("doc1", "doc2"));
        var parameters = RerankParameters.of(queryAndDocsInputs);

        assertThat(parameters.taskTypeParameters(), is(Map.of("query", "\"query_value\"")));
    }
}
