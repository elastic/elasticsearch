/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that {@code PUT _query/data_source} fails with {@code 503 Service Unavailable} when no
 * {@code EncryptionService} is bound on the node — there is no plaintext fallback. Uses a cluster
 * with {@link DataSourceCrudIT.LocalStateDataSource} but without {@link TestEncryptionServicePlugin},
 * so encryption is unavailable by construction.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 1)
public class DataSourceEncryptionRequiredIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Deliberately no TestEncryptionServicePlugin — EncryptionService stays unbound.
        return List.of(DataSourceCrudIT.LocalStateDataSource.class);
    }

    public void testPutFailsWhenEncryptionUnavailable() {
        // Even a data source with no secrets is rejected — the gate is the encryption capability of the
        // cluster, not the presence of secret-flagged settings.
        ExecutionException ee = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                PutDataSourceAction.INSTANCE,
                DataSourceCrudIT.putDataSourceRequest("ds_with_no_secret", Map.of("region", "us-east-1"))
            ).get()
        );
        Throwable cause = ee.getCause();
        assertThat(cause, instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException ese = (ElasticsearchStatusException) cause;
        assertThat(ese.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(ese.getMessage(), containsString("secret encryption service is not available"));
    }

    public void testPutWithSecretAlsoFailsWhenEncryptionUnavailable() {
        ExecutionException ee = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                PutDataSourceAction.INSTANCE,
                DataSourceCrudIT.putDataSourceRequest("ds_with_secret", Map.of("secret_access_key", "ANY"))
            ).get()
        );
        Throwable cause = ee.getCause();
        assertThat(cause, instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException ese = (ElasticsearchStatusException) cause;
        assertThat(ese.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }
}
