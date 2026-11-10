/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Contract tests for {@link EsqlAuthorizeDatasourceAction.Request} — the properties the security filter and the
 * datasource interceptor key on. {@code InlineDatasetAuthorizationIT} covers the end-to-end refusals; these pin
 * the request at the layer where a change to it is made.
 */
public class EsqlAuthorizeDatasourceActionRequestTests extends ESTestCase {

    private static EsqlAuthorizeDatasourceAction.Request request(String... dataSources) {
        return new EsqlAuthorizeDatasourceAction.Request(TEST_REQUEST_TIMEOUT, dataSources);
    }

    /**
     * The security property this request exists for. Were it {@link IndicesRequest.Replaceable}, the filter would
     * narrow {@code indices()} to the subset the caller may read and the body — which asks nothing — would answer
     * success for a data source they may not use. Fixed on its names, the whole request is refused instead.
     */
    public void testTheRequestIsNotReplaceable() {
        assertThat(request("ds1"), not(instanceOf(IndicesRequest.Replaceable.class)));
    }

    public void testBothAuthorizationHandlesNameTheSameDataSources() {
        EsqlAuthorizeDatasourceAction.Request request = request("ds1", "ds2");

        assertThat(request.indices(), arrayContaining("ds1", "ds2"));
        assertThat(request.dataSourceNames(), arrayContaining("ds1", "ds2"));
        assertThat(request.dataSourceClusterActionName(), equalTo(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME));
    }

    /**
     * A closed or hidden data source is one the caller still may not use, so the options must reach it rather than
     * expanding to nothing — and the default options error on an unavailable target.
     */
    public void testClosedAndHiddenTargetsAreStillAuthorized() {
        var wildcards = request("ds1").indicesOptions().wildcardOptions();

        assertThat(wildcards.matchClosed(), is(true));
        assertThat(wildcards.includeHidden(), is(true));
    }

    /** Cancellable, because the query that dispatched it can be. */
    public void testTheTaskIsCancellable() {
        assertThat(
            request("ds1").createTask(1L, "transport", EsqlAuthorizeDatasourceAction.NAME, TaskId.EMPTY_TASK_ID, Map.of()),
            instanceOf(CancellableTask.class)
        );
    }

    public void testNothingToValidateBeyondTheNames() {
        assertThat(request("ds1").validate(), nullValue());
        assertThat(request("ds1").toString(), containsString("ds1"));
    }

    /**
     * No data source is nothing to authorize, which would make a dispatch of this request a silent pass. Asserted
     * rather than thrown because no caller builds one: a reference always names the source it reads through.
     */
    public void testANoDataSourceRequestIsRefused() {
        expectThrows(AssertionError.class, () -> request());
    }
}
