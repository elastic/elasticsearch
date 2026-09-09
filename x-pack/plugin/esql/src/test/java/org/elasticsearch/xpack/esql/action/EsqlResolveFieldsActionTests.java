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

            // The whole options object is compared against the incoming one with only that flag flipped, rather than
            // component by component, so a component added later is covered without anyone remembering to add it here.
            var expected = IndicesOptions.builder(incoming)
                .indexAbstractionOptions(
                    IndicesOptions.IndexAbstractionOptions.builder(incoming.indexAbstractionOptions()).resolveDatasets(false)
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
