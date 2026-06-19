/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Contract tests for {@link EsqlResolveDatasetAction.Request} — the seam that makes the security filter and the
 * datasource/DLS-FLS interceptors fire on the query path. The end-to-end effect is covered by {@code EsqlSecurityIT};
 * these pin the request properties those interceptors key on, at the closest layer.
 */
public class EsqlResolveDatasetActionRequestTests extends ESTestCase {

    private static EsqlResolveDatasetAction.Request request(String[] indices, Map<String, String> datasetToDataSource) {
        return new EsqlResolveDatasetAction.Request(TEST_REQUEST_TIMEOUT, indices, datasetToDataSource);
    }

    public void testResolveDatasetsIsEnabled() {
        // resolveDatasets(true) is what routes the names through the security filter and fires the DLS/FLS interceptor;
        // resolveViews(false) keeps this off the view path.
        var options = request(new String[] { "ds1" }, Map.of("ds1", "src1")).indicesOptions().indexAbstractionOptions();
        assertThat(options.resolveDatasets(), is(true));
        assertThat(options.resolveViews(), is(false));
    }

    public void testDataSourceNamesFollowReplacedIndices() {
        // The security filter replaces indices() with the authorized subset; dataSourceNames() must then report
        // exactly the surviving datasets' parents — deduped, with names that have no registered parent dropped.
        var request = request(new String[] { "ds1", "ds2", "ds3" }, Map.of("ds1", "srcA", "ds2", "srcA", "ds3", "srcB"));
        assertThat(Set.of(request.dataSourceNames()), equalTo(Set.of("srcA", "srcB")));

        request.indices("ds1");
        assertThat(request.dataSourceNames(), arrayContaining("srcA"));

        request.indices("ds1", "unknown");
        assertThat(request.dataSourceNames(), arrayContaining("srcA"));

        request.indices(new String[0]);
        assertThat(request.dataSourceNames().length, equalTo(0));
    }

    public void testDataSourceClusterActionIsAuthorizeDatasource() {
        assertThat(
            request(new String[] { "ds1" }, Map.of("ds1", "src1")).dataSourceClusterActionName(),
            equalTo(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME)
        );
    }

    public void testValidateAcceptsRequest() {
        assertThat(request(new String[] { "ds1" }, Map.of()).validate(), nullValue());
    }
}
