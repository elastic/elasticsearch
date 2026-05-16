/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies the plaintext-fallback behavior when no {@link org.elasticsearch.xpack.core.crypto.EncryptionService}
 * is bound on the node — the producer-side path that lets ES|QL data sources work on dev clusters / laptops
 * where {@code xpack.security} isn't enabled. Cluster runs without {@link TestEncryptionServicePlugin}, so
 * the encryption holder stays null and secret PUTs land in cluster state as plaintext Strings (not encrypted
 * byte[] blobs). The single per-node-lifetime warning is logged on the first such PUT.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 1)
public class DataSourceWithoutEncryptionIT extends ESIntegTestCase {

    private static final TimeValue TEST_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Deliberately no TestEncryptionServicePlugin — EncryptionService stays unbound.
        return List.of(DataSourceCrudIT.LocalStateDataSource.class);
    }

    public void testPutWithSecretStoresPlaintextAndReportsState() throws Exception {
        final String dsName = "no_encryption_secret";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                DataSourceCrudIT.putDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIA_plaintext"))
            ).get()
        );

        GetDataSourceAction.Response resp = client().execute(
            GetDataSourceAction.INSTANCE,
            new GetDataSourceAction.Request(TEST_TIMEOUT, new String[] { dsName })
        ).get();
        assertThat(resp.getDataSources(), hasSize(1));
        DataSource ds = resp.getDataSources().iterator().next();

        DataSourceSetting secret = ds.settings().get("secret_access_key");
        assertTrue("secret flag preserved", secret.secret());
        assertThat("secret value stored as plaintext String when no service is bound", secret.rawValue(), instanceOf(String.class));
        assertEquals("plaintext value preserved verbatim", "AKIA_plaintext", secret.rawValue());
        assertThat("ds reports plaintext state", ds.encryptionState(), equalTo("plaintext"));
    }

    public void testPutWithNoSecretsSucceedsSilently() throws Exception {
        // Non-secret-only PUTs don't trigger the plaintext warning (no secrets to be at risk).
        final String dsName = "no_encryption_no_secret";
        assertAcked(
            client().execute(PutDataSourceAction.INSTANCE, DataSourceCrudIT.putDataSourceRequest(dsName, Map.of("region", "us-east-1")))
                .get()
        );
        GetDataSourceAction.Response resp = client().execute(
            GetDataSourceAction.INSTANCE,
            new GetDataSourceAction.Request(TEST_TIMEOUT, new String[] { dsName })
        ).get();
        DataSource ds = resp.getDataSources().iterator().next();
        assertThat("ds with no secrets reports no_secrets state", ds.encryptionState(), equalTo("no_secrets"));
    }
}
