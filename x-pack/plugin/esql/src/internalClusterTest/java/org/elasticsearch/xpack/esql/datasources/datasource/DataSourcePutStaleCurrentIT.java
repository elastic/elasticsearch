/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * An update that keeps an already-stored secret must not depend on which node receives it.
 *
 * <p>A client that creates a data source carrying a secret and then updates another of its settings omits
 * the secret, expecting it to be carried forward. When the update lands on a node that has not yet applied
 * the publication carrying the create, that node cannot see the stored secret and must not validate the
 * request as if none had ever been set.
 *
 * <p>A dedicated master is the only voter, so blocking a data-only node from applying cluster state
 * cannot steal quorum. The create still commits; the blocked node simply cannot ack.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataSourcePutStaleCurrentIT extends ESIntegTestCase {

    /** Short, so the blocked node's missing ack is not waited out for the full default. */
    private static final TimeValue SHORT_ACK = TimeValue.timeValueSeconds(1);
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestEncryptionServicePlugin.class, DataSourceCrudIT.LocalStateDataSource.class);
    }

    public void testUpdateKeepsTheSecretOnANodeThatHasNotYetAppliedTheCreate() throws Exception {
        internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final String master = internalCluster().getMasterName();
        final String lagging = randomFrom(dataNodes);
        final String name = "cb";

        // From here the lagging node stops applying published cluster state, so it never sees the create.
        final BlockClusterStateProcessing blocked = new BlockClusterStateProcessing(lagging, random());
        internalCluster().setDisruptionScheme(blocked);
        blocked.startDisrupting();
        try {
            AcknowledgedResponse created = client(master).execute(
                PutDataSourceAction.INSTANCE,
                request(name, Map.of("region", "us-east-1", "secret_access_key", "AKIAXYZ"))
            ).actionGet(TIMEOUT);
            assertThat("the blocked node cannot ack", created.isAcknowledged(), equalTo(false));

            // The update omits the secret, which the master can carry forward. It must not be refused.
            client(lagging).execute(PutDataSourceAction.INSTANCE, request(name, Map.of("region", "eu-west-2"))).actionGet(TIMEOUT);

            // GET is a local read: ask the master, which holds the committed metadata, while the
            // lagging node is still blocked.
            GetDataSourceAction.Response got = client(master).execute(
                GetDataSourceAction.INSTANCE,
                new GetDataSourceAction.Request(TIMEOUT, new String[] { name })
            ).actionGet(TIMEOUT);
            assertThat(got.getDataSources(), hasSize(1));
            DataSource ds = got.getDataSources().iterator().next();
            assertThat(ds.settings().get("region").nonSecretValue(), equalTo("eu-west-2"));
            assertThat(decryptSecret(ds.settings().get("secret_access_key")), equalTo("AKIAXYZ"));
        } finally {
            blocked.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
    }

    private static PutDataSourceAction.Request request(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TIMEOUT, SHORT_ACK, name, "test_requires_secret", null, new HashMap<>(settings));
    }

    private static Object decryptSecret(DataSourceSetting secret) {
        DataSourceCredentials credentials = new DataSourceCredentials(new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData(TestEncryptionServicePlugin.TEST_KEY_ID, bytes);
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        });
        Map<String, Object> input = new HashMap<>();
        input.put("secret", secret.rawValue());
        return credentials.decryptInPlace(input).get("secret");
    }
}
