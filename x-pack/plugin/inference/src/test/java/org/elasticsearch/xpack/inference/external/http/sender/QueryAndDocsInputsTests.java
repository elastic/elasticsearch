/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.inference.RerankRequest.SUPPORTED_RERANK_DATA_TYPES;
import static org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs.fromRerankRequest;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class QueryAndDocsInputsTests extends ESTestCase {
    public void testMinimalConstructor() {
        var query = createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES);
        var inputs = randomList(5, () -> createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES));
        var queryAndDocsInputs = new QueryAndDocsInputs(query, inputs);

        assertThat(queryAndDocsInputs.getQuery(), is(query));
        assertThat(queryAndDocsInputs.getDocs(), is(inputs));
        assertThat(queryAndDocsInputs.getTopN(), is(nullValue()));
        assertThat(queryAndDocsInputs.getReturnDocuments(), is(nullValue()));
        assertThat(queryAndDocsInputs.stream(), is(false));
    }

    public void testFromRerankRequest_ConstructsClassCorrectly() {
        var inputs = randomList(5, () -> createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES));
        var query = createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES);
        var topN = randomIntOrNull();
        var returnDocuments = randomOptionalBoolean();
        var queryAndDocsInputs = fromRerankRequest(
            new RerankRequest(inputs, query, topN, returnDocuments, Map.of(randomAlphaOfLength(8), randomAlphaOfLength(8)))
        );

        assertThat(queryAndDocsInputs.getQuery(), is(query));
        assertThat(queryAndDocsInputs.getDocs(), is(inputs));
        assertThat(queryAndDocsInputs.getTopN(), is(topN));
        assertThat(queryAndDocsInputs.getReturnDocuments(), is(returnDocuments));
        assertThat(queryAndDocsInputs.stream(), is(false));
    }

    public void testGetQueryAsString_NonTextValue_Throws() {
        var nonTextInferenceString = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        var inputs = new QueryAndDocsInputs(nonTextInferenceString, List.of());
        var exception = expectThrows(AssertionError.class, inputs::getQueryAsString);
        assertThat(exception.getMessage(), is("Non-text input returned from InferenceString.textValue"));
    }

    public void testGetDocsAsStrings_NonTextValue_Throws() {
        // Create a list with text-only InferenceStrings
        var inputList = randomList(5, () -> createRandomUsingDataTypes(EnumSet.of(DataType.TEXT)));
        // Insert a non-text InferenceString randomly in the list
        var nonTextInferenceString = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        inputList.add(randomInt(inputList.size()), nonTextInferenceString);
        var inputs = new QueryAndDocsInputs(createRandomUsingDataTypes(EnumSet.of(DataType.TEXT)), inputList);
        var exception = expectThrows(AssertionError.class, inputs::getDocsAsStrings);
        assertThat(exception.getMessage(), is("Non-text input returned from InferenceString.textValue"));
    }

    public void testIsSingleInput() {
        var singleInput = new QueryAndDocsInputs(
            createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES),
            List.of(createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES))
        );
        assertThat(singleInput.isSingleInput(), is(true));

        var multipleInputs = new QueryAndDocsInputs(
            createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES),
            randomList(2, 5, () -> createRandomUsingDataTypes(SUPPORTED_RERANK_DATA_TYPES))
        );
        assertThat(multipleInputs.isSingleInput(), is(false));
    }
}
