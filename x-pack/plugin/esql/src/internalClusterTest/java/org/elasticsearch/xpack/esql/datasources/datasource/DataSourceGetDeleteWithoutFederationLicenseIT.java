/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.GetDatasetAction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that GET and DELETE operations on datasets and data sources succeed on a cluster with a Basic
 * (non-Enterprise) license. These actions previously called {@code federationLicense.check()}, which would
 * reject any request from non-Enterprise clusters. The check was intentionally removed so that datasets and
 * data sources created before a license downgrade can still be listed and deleted.
 *
 * <p>Each test calls the relevant action and asserts it either returns an empty result or throws
 * {@link ResourceNotFoundException} — not an {@code ElasticsearchStatusException} with "Enterprise license
 * required", which is what a mistakenly re-introduced license check would produce.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 1)
public class DataSourceGetDeleteWithoutFederationLicenseIT extends ESIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestEncryptionServicePlugin.class, DataSourceCrudIT.LocalStateDataSource.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.license.self_generated.type", "basic")
            .build();
    }

    /**
     * GET /_query/datasource/* on an empty registry must return an empty list, not a license error.
     */
    public void testGetDataSourceReturnsEmptyWithoutFederationLicense() throws Exception {
        GetDataSourceAction.Response resp = client().execute(
            GetDataSourceAction.INSTANCE,
            new GetDataSourceAction.Request(TIMEOUT, new String[] { "*" })
        ).get();
        assertThat(resp.getDataSources(), hasSize(0));
    }

    /**
     * GET /_query/dataset/* on an empty registry must return an empty list, not a license error.
     */
    public void testGetDatasetReturnsEmptyWithoutFederationLicense() throws Exception {
        GetDatasetAction.Request req = new GetDatasetAction.Request(TIMEOUT);
        req.indices("*");
        GetDatasetAction.Response resp = client().execute(GetDatasetAction.INSTANCE, req).get();
        assertThat(resp.getDatasets(), hasSize(0));
    }

    /**
     * DELETE /_query/dataset/nonexistent on a Basic cluster must throw {@link ResourceNotFoundException},
     * not a license error, confirming the license check is not present.
     */
    public void testDeleteDatasetThrowsNotFoundWithoutFederationLicense() {
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                DeleteDatasetAction.INSTANCE,
                new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { "nonexistent_dataset" })
            ).get()
        );
        assertThat(
            "expected ResourceNotFoundException (no license check), not a license error",
            err.getCause(),
            instanceOf(ResourceNotFoundException.class)
        );
    }

    /**
     * DELETE /_query/datasource/nonexistent on a Basic cluster must throw {@link ResourceNotFoundException},
     * not a license error, confirming the license check is not present.
     */
    public void testDeleteDataSourceThrowsNotFoundWithoutFederationLicense() {
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { "nonexistent_ds" })
            ).get()
        );
        assertThat(
            "expected ResourceNotFoundException (no license check), not a license error",
            err.getCause(),
            instanceOf(ResourceNotFoundException.class)
        );
    }
}
