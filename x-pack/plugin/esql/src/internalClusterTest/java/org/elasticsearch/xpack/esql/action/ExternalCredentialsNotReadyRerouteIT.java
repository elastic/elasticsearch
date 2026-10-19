/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * A worker that cannot decrypt the data source credentials yet must not fail the query: it declines before reading anything, and the
 * coordinator sends its splits to another worker. This is the ES|QL shape of the general rule that a receiver fails fast with a typed
 * reason and the sender moves the work, and it is what a freshly joined node hits during scale-up while it is still applying the
 * cluster state that carries the key.
 *
 * <p>The key miss is injected: the node's {@link EncryptionService} reports "key not yet available" on one chosen node. The miss is
 * decided from the current thread's name, which carries the node name, so it does not depend on which node's service instance the
 * static {@link EncryptionServiceRegistry} handed to each {@code EsqlPlugin} in this shared test JVM. Data lives in local parquet
 * files read through {@code file://}; the data source carries a {@code secret_token} so every worker has to decrypt before it reads.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalCredentialsNotReadyRerouteIT extends AbstractExternalDataSourceIT {

    /** Encrypt wraps the bytes under a fixed key id; decrypt unwraps everywhere except on {@link #nodeWithoutKey}. */
    public static class KeyMissingOnOneNodeEncryptionPlugin extends Plugin {
        static volatile String nodeWithoutKey;
        static final AtomicInteger declinedDecrypts = new AtomicInteger();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            EncryptionService svc = new EncryptionService() {
                @Override
                public EncryptedData encrypt(byte[] bytes) {
                    return new EncryptedData("test-key", bytes.clone());
                }

                @Override
                public byte[] decrypt(EncryptedData encryptedData) {
                    String missing = nodeWithoutKey;
                    if (missing != null && Thread.currentThread().getName().contains("[" + missing + "]")) {
                        declinedDecrypts.incrementAndGet();
                        throw new EncryptionKeyNotYetAvailableException(
                            "project encryption key [{}] is not yet available",
                            encryptedData.keyId()
                        );
                    }
                    return encryptedData.payload();
                }
            };
            EncryptionServiceRegistry.setEncryptionService(svc);
            return List.of(new PluginComponentBinding<>(EncryptionService.class, svc));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // Replace the base class's identity stub: a node may bind EncryptionService only once. Ours goes first so it is constructed
        // before EsqlPlugin reads the static registry on each node.
        plugins.remove(TestEncryptionServicePlugin.class);
        plugins.add(0, KeyMissingOnOneNodeEncryptionPlugin.class);
        return plugins;
    }

    @Before
    public void resetStubs() {
        KeyMissingOnOneNodeEncryptionPlugin.nodeWithoutKey = null;
        KeyMissingOnOneNodeEncryptionPlugin.declinedDecrypts.set(0);
    }

    public void testSplitsMoveToAnotherWorkerWhenOneCannotDecryptYet() throws Exception {
        int fileCount = 6;
        String dataset = registerSecretParquetDataset(fileCount);
        String[] nodes = internalCluster().getNodeNames();
        String coordinator = nodes[0];
        String nodeWithoutKey = nodes[1];
        KeyMissingOnOneNodeEncryptionPlugin.nodeWithoutKey = nodeWithoutKey;

        try (EsqlQueryResponse response = runDistributed(coordinator, dataset)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat("every file is read exactly once", rows.get(0).get(0), equalTo((long) fileCount));
            assertThat(rows.get(0).get(1), equalTo((long) fileCount * (fileCount - 1) / 2));
            Set<String> scanNodes = externalScanNodeNames(response);
            assertThat("the node without the key ran no scan", scanNodes, not(hasItem(nodeWithoutKey)));
            assertThat(scanNodes, not(empty()));
        }
        assertThat(
            "the node without the key was asked and declined",
            KeyMissingOnOneNodeEncryptionPlugin.declinedDecrypts.get(),
            greaterThanOrEqualTo(1)
        );
    }

    public void testEveryWorkerServesWhenAllHoldTheKey() throws Exception {
        int fileCount = 6;
        String dataset = registerSecretParquetDataset(fileCount);

        try (EsqlQueryResponse response = runDistributed(internalCluster().getNodeNames()[0], dataset)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo((long) fileCount));
        }
        assertThat(KeyMissingOnOneNodeEncryptionPlugin.declinedDecrypts.get(), equalTo(0));
    }

    /** One single-row parquet file per split, under a data source that carries an encrypted secret. */
    private String registerSecretParquetDataset(int fileCount) throws Exception {
        Path dir = createTempDir().resolve("reroute");
        Files.createDirectories(dir);
        for (int i = 0; i < fileCount; i++) {
            int value = i;
            writeParquet(
                dir.resolve("f" + i + ".parquet"),
                "message test { required int32 value; }",
                1,
                1024,
                (g, r) -> g.add("value", value)
            );
        }
        registerDataSource("secret_src", Map.of("secret_token", "S3CR3T-" + randomAlphaOfLength(6)));
        String uri = StoragePath.fileUri(dir) + "/*.parquet";
        registerDataset("reroute_ds", "secret_src", uri, Map.of());
        return "reroute_ds";
    }

    /** Forces the splits onto remote workers, round robin, so the node without the key is certain to be asked. */
    private EsqlQueryResponse runDistributed(String coordinator, String dataset) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*), s = SUM(value)").pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        return client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT);
    }
}
