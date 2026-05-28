/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Proves end-to-end that the per-data-source secret reaches a data node other than the coordinator and is
 * decrypted there at storage-provider construction time. Strategy:
 * <ol>
 *   <li>Start a single-node cluster (master + data combined).</li>
 *   <li>Register a data source + dataset while still single-node (the full-state path that registers
 *     cleanly).</li>
 *   <li>Dynamically add a data-only node; it joins by receiving the full cluster state in one shot,
 *     avoiding the incremental-publication assertion that fires when a dataset is added to an already
 *     multi-node cluster.</li>
 *   <li>Force {@code external_distribution=round_robin} so the read fans out across both data nodes.</li>
 *   <li>The capturing storage-provider factory records {@code node-name -> plaintext secret} as it
 *     observes it. The test asserts the secret reached the joined data node (not just the coordinator)
 *     and was decrypted.</li>
 * </ol>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FileSourceSecretDecryptionAcrossNodesIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String SECRET_KEY = "secret_token";
    private static final String SECRET_VALUE = "S3CR3T_across_nodes";
    private static final String SCHEME = "teststorexn";

    /** node-name -> captured secret value at provider construction. */
    static final Map<String, String> capturedByNode = new ConcurrentHashMap<>();

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    public static final class CapturingDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedSchemes() {
            return Set.of(SCHEME);
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of(SCHEME, new CapturingStorageProviderFactory());
        }
    }

    private static final class CapturingStorageProviderFactory implements StorageProviderFactory {
        @Override
        public StorageProvider create(Settings settings) {
            return new LocalFileStorageProvider();
        }

        @Override
        public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
            Object secret = config == null ? null : config.get(SECRET_KEY);
            String node = settings.get("node.name", "unknown");
            if (secret instanceof String s) {
                capturedByNode.put(node, s);
            } else if (secret instanceof EncryptedData) {
                // Loud signal if a data node ever sees an undecrypted carrier: the whole point of this PR
                // is that the registry decrypts at the chokepoint before the factory is called.
                throw new IllegalStateException("node [" + node + "] received an UNDECRYPTED secret carrier: " + secret);
            }
            return new Configured<>(new LocalFileStorageProvider(), secret == null ? Set.of() : Set.of(SECRET_KEY));
        }
    }

    private static final class LocalFileStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new LocalDelegateObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new LocalDelegateObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new LocalDelegateObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public StorageEntry next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public boolean exists(StoragePath path) {
            return Files.exists(localPathOf(path));
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of(SCHEME);
        }

        @Override
        public void close() {}
    }

    @SuppressForbidden(reason = "test converts a teststorexn URI to a local Path to serve fixture bytes")
    private static Path localPathOf(StoragePath path) {
        return PathUtils.get(path.localPath());
    }

    private static final class LocalDelegateObject implements StorageObject {
        private final StoragePath path;
        private final Path file;

        LocalDelegateObject(StoragePath path) {
            this.path = path;
            this.file = localPathOf(path);
        }

        @Override
        public InputStream newStream() throws IOException {
            return Files.newInputStream(file);
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            InputStream in = Files.newInputStream(file);
            long skipped = 0;
            while (skipped < position) {
                long n = in.skip(position - skipped);
                if (n <= 0) {
                    break;
                }
                skipped += n;
            }
            return in;
        }

        @Override
        public long length() throws IOException {
            return Files.size(file);
        }

        @Override
        public Instant lastModified() throws IOException {
            return Files.getLastModifiedTime(file).toInstant();
        }

        @Override
        public boolean exists() {
            return Files.exists(file);
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void resetCapture() {
        capturedByNode.clear();
    }

    // No @After cleanup: TEST scope rebuilds the cluster per method, and deleting the dataset on the now
    // multi-node cluster would trip a pre-existing main assertion in the publication path (the same bug
    // that blocks a static numDataNodes >= 2 form of this test).

    public void testSecretReachesAndIsDecryptedOnNonCoordinatorDataNode() throws Exception {
        // Step 1 — record the coordinator (the only node so far) for later differentiation.
        String coordinatorName = internalCluster().getNodeNames()[0];

        Path fixture = createTempFile("secret-fixture-", ".csv");
        Files.writeString(fixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob") + "\n");

        // Step 2 — register the data source (encrypts) and the dataset while single-node, so dataset
        // publication does not trip the incremental-publication assertion that fires on multi-node.
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "secret_src",
                    "test",
                    null,
                    new HashMap<>(Map.of(SECRET_KEY, SECRET_VALUE))
                )
            )
        );
        String uri = SCHEME + "://" + StoragePath.fileUri(fixture).substring("file://".length());
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, "secret_ds", "secret_src", uri, null, new HashMap<>(Map.of("format", "csv")))
            )
        );

        // Step 3 — add a data-only node; it bootstraps with the cluster state in one shot.
        String dataNodeName = internalCluster().startDataOnlyNode();
        ensureGreen();

        // Step 4 — query. round_robin distributes the splits across data nodes.
        try (var ignored = run(syncEsqlQueryRequest("FROM secret_ds | LIMIT 5"), TIMEOUT)) {
            // consumed for side effects
        }

        logger.info("coordinator=[{}] dataNode=[{}] capturedByNode={}", coordinatorName, dataNodeName, capturedByNode);

        // Step 5 — the read must have built the storage client on the data-only node (not just the
        // coordinator) and the secret must have arrived there as plaintext.
        assertTrue(
            "expected the data-only node [" + dataNodeName + "] to observe the secret; observed nodes=" + capturedByNode.keySet(),
            capturedByNode.containsKey(dataNodeName)
        );
        assertEquals(
            "data-only node [" + dataNodeName + "] must receive the decrypted plaintext secret",
            SECRET_VALUE,
            capturedByNode.get(dataNodeName)
        );
    }
}
