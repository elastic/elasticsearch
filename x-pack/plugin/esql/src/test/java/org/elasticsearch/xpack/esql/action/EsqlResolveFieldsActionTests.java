/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.IndexAbstractionOptions;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class EsqlResolveFieldsActionTests extends ESTestCase {

    /**
     * The remote field-caps gate: the outgoing request resolves datasets iff wildcards may match them, always resolves
     * views, and leaves the rest of the base abstraction options (here: resolveAliases) untouched. Guards against the
     * bit being re-hardcoded or the wrong abstraction being gated.
     */
    public void testRemoteResolveOptionsGatesDatasetsOnly() {
        // Base with every abstraction bit off, so a flipped bit in the result is unambiguously the gate's doing.
        IndicesOptions base = IndicesOptions.builder(IndicesOptions.DEFAULT)
            .indexAbstractionOptions(new IndexAbstractionOptions(false, false, false))
            .build();

        IndexAbstractionOptions off = EsqlResolveFieldsAction.remoteResolveOptions(base, false).indexAbstractionOptions();
        assertThat("wildcards off => datasets not resolved", off.resolveDatasets(), equalTo(false));
        assertThat("views always resolved", off.resolveViews(), equalTo(true));
        assertThat("base aliases option preserved", off.resolveAliases(), equalTo(false));

        IndexAbstractionOptions on = EsqlResolveFieldsAction.remoteResolveOptions(base, true).indexAbstractionOptions();
        assertThat("wildcards on => datasets resolved", on.resolveDatasets(), equalTo(true));
        assertThat("views always resolved", on.resolveViews(), equalTo(true));
    }
}
