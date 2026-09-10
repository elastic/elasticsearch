/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class EsqlResolveFieldsActionTests extends ESTestCase {

    /**
     * The receiving cluster refuses to resolve its own datasets even when the caller asked it to, which is what makes a
     * remote dataset invisible to a coordinator that predates this change and still sets the flag. Nothing else on the
     * request may move: the clear is reached after the security layer has already resolved indices under the incoming
     * flag, so anything it touched beyond this one option would be a second, unrelated change to a resolved request.
     */
    public void testDatasetResolutionIsClearedWhateverTheCallerAsked() {
        for (boolean callerAsked : new boolean[] { true, false }) {
            var request = new FieldCapabilitiesRequest().indices("remote_employees");
            var incoming = IndicesOptions.builder(randomIndicesOptions())
                .indexAbstractionOptions(
                    IndicesOptions.IndexAbstractionOptions.builder().resolveViews(randomBoolean()).resolveDatasets(callerAsked)
                )
                .build();
            request.indicesOptions(incoming);

            EsqlResolveFieldsAction.clearDatasetResolution(request);

            // The expectation names the two sibling flags outright rather than copying them through the same builder
            // the clear uses. Rebuilding it the same way would make both sides drop anything the copy constructor
            // failed to carry, and the assertion would stay green on exactly the drift it is here to catch.
            var expected = IndicesOptions.builder(incoming)
                .indexAbstractionOptions(
                    new IndicesOptions.IndexAbstractionOptions(
                        incoming.indexAbstractionOptions().resolveAliases(),
                        incoming.indexAbstractionOptions().resolveViews(),
                        false
                    )
                )
                .build();
            assertThat(request.indicesOptions(), equalTo(expected));
            assertThat(request.indicesOptions().indexAbstractionOptions().resolveDatasets(), equalTo(false));
            assertThat(request.indices(), equalTo(new String[] { "remote_employees" }));
        }
    }

    private static IndicesOptions randomIndicesOptions() {
        return randomFrom(
            IndicesOptions.DEFAULT,
            IndicesOptions.strictExpandOpen(),
            IndicesOptions.lenientExpandOpen(),
            IndicesOptions.strictSingleIndexNoExpandForbidClosed()
        );
    }
}
