/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies the no-{@link org.elasticsearch.xpack.encryption.spi.EncryptionService} behavior. Encryption is
 * mandatory for storing secrets: a node with no service bound rejects a secret-bearing PUT with {@code 503}
 * rather than persisting plaintext credentials to cluster state. Data sources that carry no secrets need no
 * service and are stored as-is, so credential-free data sources still work on a cluster without security.
 * The cluster runs without {@link TestEncryptionServicePlugin}, so the encryption holder stays null.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 1)
public class DataSourceWithoutEncryptionIT extends ESIntegTestCase {

    private static final TimeValue TEST_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Deliberately no TestEncryptionServicePlugin — EncryptionService stays unbound.
        return List.of(DataSourceCrudIT.LocalStateDataSource.class);
    }

    public void testPutWithSecretIsRejected() {
        final String dsName = "no_encryption_secret";
        ExecutionException ee = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                PutDataSourceAction.INSTANCE,
                DataSourceCrudIT.putDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIA_plaintext"))
            ).get()
        );
        assertThat(ee.getCause(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException ese = (ElasticsearchStatusException) ee.getCause();
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ese.status());
        assertThat(ese.getMessage(), containsString("no encryption service is available"));

        // The rejected PUT must not have created the data source: GET by name 404s on a missing source.
        ExecutionException getErr = expectThrows(
            ExecutionException.class,
            () -> client().execute(GetDataSourceAction.INSTANCE, new GetDataSourceAction.Request(TEST_TIMEOUT, new String[] { dsName }))
                .get()
        );
        assertThat(getErr.getCause(), instanceOf(ResourceNotFoundException.class));
    }

    public void testPutWithoutSecretSucceeds() throws Exception {
        // A credential-free data source needs no encryption service and is stored as-is.
        final String dsName = "no_encryption_no_secret";
        assertAcked(
            safeGet(
                client().execute(PutDataSourceAction.INSTANCE, DataSourceCrudIT.putDataSourceRequest(dsName, Map.of("region", "us-east-1")))
            )
        );

        GetDataSourceAction.Response resp = client().execute(
            GetDataSourceAction.INSTANCE,
            new GetDataSourceAction.Request(TEST_TIMEOUT, new String[] { dsName })
        ).get();
        assertThat(resp.getDataSources(), hasSize(1));
        DataSource ds = resp.getDataSources().iterator().next();
        assertThat(ds.settings().get("region").nonSecretValue(), equalTo("us-east-1"));
    }
}
