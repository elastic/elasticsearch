/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Contract tests for {@link EsqlResolveDatasetAction.Request} — the seam that makes the security filter and the
 * DLS/FLS interceptor fire on the query path. The end-to-end effect is covered by {@code EsqlSecurityIT}; these pin
 * the request properties those interceptors key on, at the closest layer.
 */
public class EsqlResolveDatasetActionRequestTests extends ESTestCase {

    private static EsqlResolveDatasetAction.Request request(String... indices) {
        return new EsqlResolveDatasetAction.Request(TEST_REQUEST_TIMEOUT, indices);
    }

    public void testResolveDatasetsIsEnabled() {
        // resolveDatasets(true) is what routes the names through the security filter and fires the DLS/FLS interceptor;
        // resolveViews(false) keeps this off the view path.
        var options = request("ds1").indicesOptions().indexAbstractionOptions();
        assertThat(options.resolveDatasets(), is(true));
        assertThat(options.resolveViews(), is(false));
    }

    public void testIndicesAreReplaceable() {
        // The security filter narrows indices() to the authorized subset in transit; the body reads that back.
        var request = request("ds1", "ds2");
        assertThat(request.indices(), arrayContaining("ds1", "ds2"));
        request.indices("ds1");
        assertThat(request.indices(), arrayContaining("ds1"));
    }

    public void testRawPatternsSurviveIndicesNarrowing() {
        // rawPatterns() must keep the ORIGINAL FROM patterns even after the filter narrows indices() to the authorized
        // subset — the action body needs them to classify whether the relation also targets non-dataset abstractions.
        var request = request("logs_*", "-logs_test");
        assertThat(request.rawPatterns(), arrayContaining("logs_*", "-logs_test"));
        request.indices("logs_a");
        assertThat(request.indices(), arrayContaining("logs_a"));
        assertThat("rawPatterns is unaffected by indices() narrowing", request.rawPatterns(), arrayContaining("logs_*", "-logs_test"));
    }

    public void testUnavailableTargetsAreLenient() {
        // Lenient options: the security filter silently narrows an unauthorized concrete dataset to nothing rather than
        // throwing a 403 (which would be an existence oracle). The explicit-unauthorized → Unknown index (400) is
        // surfaced by the rewrite instead.
        var options = request("ds1").indicesOptions();
        assertThat(options.ignoreUnavailable(), is(true));
        assertThat(options.allowNoIndices(), is(true));
    }

    public void testValidateAcceptsRequest() {
        assertThat(request("ds1").validate(), nullValue());
    }
}
