/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies the plaintext-fallback behavior when no {@link org.elasticsearch.xpack.encryption.spi.EncryptionService}
 * is bound on the node — the producer-side path that lets ES|QL data sources work on dev clusters / laptops
 * where {@code xpack.security} isn't enabled. Cluster runs without {@link TestEncryptionServicePlugin}, so
 * the encryption holder stays null and secret PUTs land in cluster state as plaintext Strings (not encrypted
 * byte[] blobs). A {@code WARN} naming the data source fires on every PUT that carries at least one secret
 * (so the operator can see exactly which credentials hit the disk in clear text, every time).
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 1)
public class DataSourceWithoutEncryptionIT extends ESIntegTestCase {

    private static final TimeValue TEST_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Deliberately no TestEncryptionServicePlugin — EncryptionService stays unbound.
        return List.of(DataSourceCrudIT.LocalStateDataSource.class);
    }

    public void testPutWithSecretStoresPlaintext() throws Exception {
        final String dsName = "no_encryption_secret";
        // The WARN naming the data source must fire on every PUT carrying a secret when no
        // EncryptionService is bound — operators rely on it as the per-PUT plaintext-storage signal.
        MockLog.assertThatLogger(
            () -> assertAcked(
                safeGet(
                    client().execute(
                        PutDataSourceAction.INSTANCE,
                        DataSourceCrudIT.putDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIA_plaintext"))
                    )
                )
            ),
            DataSourceService.class,
            new MockLog.SeenEventExpectation(
                "plaintext-storage WARN naming the data source",
                DataSourceService.class.getCanonicalName(),
                Level.WARN,
                "credentials for data source [" + dsName + "] are stored as plaintext because no encryption service is available"
            )
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
    }

    public void testPutWithoutSecretDoesNotLogWarn() throws Exception {
        // Mirror of the secret-PUT case: a PUT with no secret-classified settings must NOT log the
        // plaintext-storage WARN. The operator signal exists only when real credentials hit the disk.
        final String dsName = "no_encryption_no_secret";
        MockLog.assertThatLogger(
            () -> assertAcked(
                safeGet(
                    client().execute(
                        PutDataSourceAction.INSTANCE,
                        DataSourceCrudIT.putDataSourceRequest(dsName, Map.of("region", "us-east-1"))
                    )
                )
            ),
            DataSourceService.class,
            new MockLog.UnseenEventExpectation(
                "no plaintext-storage WARN when the PUT carries no secret",
                DataSourceService.class.getCanonicalName(),
                Level.WARN,
                "*credentials for data source*are stored as plaintext*"
            )
        );
    }
}
