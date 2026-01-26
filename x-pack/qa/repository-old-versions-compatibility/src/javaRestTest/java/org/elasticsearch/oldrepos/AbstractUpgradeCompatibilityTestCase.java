/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.test.cluster.util.Version.CURRENT;
import static org.elasticsearch.test.cluster.util.Version.fromString;
import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@TestCaseOrdering(AbstractUpgradeCompatibilityTestCase.TestCaseOrdering.class)
public class AbstractUpgradeCompatibilityTestCase extends ESRestTestCase {

    protected static final Version VERSION_MINUS_2 = fromString(System.getProperty("tests.minimum.index.compatible"));
    protected static final Version VERSION_MINUS_1 = fromString(System.getProperty("tests.minimum.wire.compatible"));
    protected static final Version VERSION_CURRENT = CURRENT;

    protected static TemporaryFolder REPOSITORY_PATH = new TemporaryFolder();

    protected static LocalClusterConfigProvider clusterConfig = c -> {};
    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(VERSION_MINUS_1)
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("path.repo", () -> REPOSITORY_PATH.getRoot().getPath())
        .apply(() -> clusterConfig)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(REPOSITORY_PATH).around(cluster);

    private static boolean upgradeFailed = false;

    private final Version clusterVersion;

    public AbstractUpgradeCompatibilityTestCase(@Name("cluster") Version clusterVersion) {
        this.clusterVersion = clusterVersion;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Stream.of(VERSION_MINUS_1, CURRENT).map(v -> new Object[] { v }).toList();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * This method verifies the currentVersion against the clusterVersion and performs a "full cluster restart" upgrade if the current
     * is before clusterVersion. The cluster version is fetched externally and is controlled by the gradle setup.
     *
     * @throws Exception
     */
    @Before
    public void maybeUpgrade() throws Exception {
        // We want to use this test suite for the V9 upgrade, but we are not fully committed to necessarily having N-2 support
        // in V10, so we add a check here to ensure we'll revisit this decision once V10 exists.
        assertThat("Explicit check that N-2 version is Elasticsearch 7", VERSION_MINUS_2.getMajor(), equalTo(7));

        var currentVersion = clusterVersion();
        if (currentVersion.before(clusterVersion)) {
            try {
                cluster.upgradeToVersion(clusterVersion);
                closeClients();
                initClient();
            } catch (Exception e) {
                upgradeFailed = true;
                throw e;
            }
        }

        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);
    }

    private static Version clusterVersion() throws Exception {
        var response = assertOK(client().performRequest(new Request("GET", "/")));
        var responseBody = createFromResponse(response);
        var version = Version.fromString(responseBody.evaluate("version.number").toString());
        assertThat("Failed to retrieve cluster version", version, notNullValue());
        return version;
    }

    /**
     * Execute the test suite with the parameters provided by the {@link #parameters()} in version order.
     */
    public static class TestCaseOrdering implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
            var version1 = (Version) o1.getInstanceArguments().get(0);
            var version2 = (Version) o2.getInstanceArguments().get(0);
            return version1.compareTo(version2);
        }
    }

    /**
     * This method executes in two phases. For cluster in version Current-1, restores and mounts an index snapshot and performs assertions.
     * For cluster in version Current, asserts the previously restored/mounted index exists in the upgraded setup.
     *
     * @param extension  The snapshot suffix in resources covering different scenarios.
     * @throws Exception
     */
    protected final void verifyCompatibility(String extension) throws Exception {
        final String repository = "repository_" + extension;
        final String index = "index_" + extension;
        final String indexMount = index + "_" + repository;
        final int numDocs = 1;

        String repositoryPath = REPOSITORY_PATH.getRoot().getPath();

        if (VERSION_MINUS_1.equals(clusterVersion())) {
            assertEquals(VERSION_MINUS_1, clusterVersion());
            restoreMountAndAssertSnapshot(extension, repository, repositoryPath, index, indexMount, numDocs, o -> {});
        }

        if (VERSION_CURRENT.equals(clusterVersion())) {
            assertEquals(VERSION_CURRENT, clusterVersion());
            assertTrue(getIndices(client()).contains(index));
            assertDocCount(client(), index, numDocs);

            assertTrue(getIndices(client()).contains(indexMount));
            assertDocCount(client(), indexMount, numDocs);
        }
    }

    /**
     * This method executes in two phases. For cluster in version Current-1 it does not do anything. For cluster in version Current,
     * restores and mounts an index snapshot and performs assertions. The test is performed only for the Current cluster.
     *
     * @param extension  The snapshot suffix in resources covering different scenarios.
     * @throws Exception
     */
    protected final void verifyCompatibilityNoUpgrade(String extension) throws Exception {
        verifyCompatibilityNoUpgrade(extension, o -> {});
    }

    protected final void verifyCompatibilityNoUpgrade(String extension, Consumer<List<String>> warningsConsumer) throws Exception {

        if (VERSION_CURRENT.equals(clusterVersion())) {
            String repository = "repository_" + extension;
            String index = "index_" + extension;
            String indexMount = index + "_" + repository;
            int numDocs = 1;

            assertEquals(VERSION_CURRENT, clusterVersion());
            String repositoryPath = REPOSITORY_PATH.getRoot().getPath();
            restoreMountAndAssertSnapshot(extension, repository, repositoryPath, index, indexMount, numDocs, warningsConsumer);
        }
    }

    private void restoreMountAndAssertSnapshot(
        String extension,
        String repository,
        String repositoryPath,
        String index,
        String indexMount,
        int numDocs,
        Consumer<List<String>> warningsConsumer
    ) throws Exception {
        copySnapshotFromResources(repositoryPath, extension);
        registerRepository(client(), repository, FsRepository.TYPE, true, Settings.builder().put("location", repositoryPath).build());

        restore(client(), repository, index, warningsConsumer);
        mount(client(), repository, index, indexMount, o -> {});

        assertTrue(getIndices(client()).contains(index));
        assertDocCount(client(), index, numDocs);
        assertPhraseQuery(client(), index, "Elasticsearch Doc");

        assertTrue(getIndices(client()).contains(indexMount));
    }

    private static void assertPhraseQuery(RestClient client, String indexName, String phrase) throws IOException {
        var request = new Request("GET", "/" + indexName + "/_search");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        request.setJsonEntity(Strings.format("""
            {
              "query": {
                "match": {
                  "content": "%s"
                }
              }
            }""", phrase));
        Response response = client.performRequest(request);
        Map<String, Object> map = responseAsMap(response);
        int hits = ((List<?>) ((Map<?, ?>) map.get("hits")).get("hits")).size();
        assertEquals("expected 1  documents but it was a different number", 1, hits);
    }

    private static String getIndices(RestClient client) throws IOException {
        final Request request = new Request("GET", "_cat/indices");
        Response response = client.performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }

    private void restore(RestClient client, String repository, String index, Consumer<List<String>> warningsConsumer) throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/snapshot/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(Strings.format("""
            {
              "indices": "%s",
              "include_global_state": false,
              "rename_pattern": "(.+)",
              "include_aliases": false
            }""", index));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client.performRequest(request);
        assertOK(response);
        warningsConsumer.accept(response.getWarnings());
    }

    private void mount(RestClient client, String repository, String index, String indexMount, Consumer<List<String>> warningsConsumer)
        throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/snapshot/_mount");
        request.addParameter("wait_for_completion", "true");
        request.addParameter("storage", "full_copy");
        request.setJsonEntity(Strings.format("""
             {
              "index": "%s",
              "renamed_index": "%s"
            }""", index, indexMount));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client.performRequest(request);
        assertOK(response);
        warningsConsumer.accept(response.getWarnings());
    }

    private static void copySnapshotFromResources(String repositoryPath, String version) throws IOException, URISyntaxException {
        Path zipFilePath = Paths.get(
            Objects.requireNonNull(AbstractUpgradeCompatibilityTestCase.class.getClassLoader().getResource("snapshot_v" + version + ".zip"))
                .toURI()
        );
        unzip(zipFilePath, Paths.get(repositoryPath));
    }

    private static void unzip(Path zipFilePath, Path outputDir) throws IOException {
        try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zipFilePath))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path outputPath = outputDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(outputPath);
                } else {
                    Files.createDirectories(outputPath.getParent());
                    try (OutputStream out = Files.newOutputStream(outputPath)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zipIn.read(buffer)) > 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipIn.closeEntry();
            }
        }
    }

    protected static final class TestSnapshotCases {
        // Index created in vES_5 - Basic mapping
        public static final String ES_VERSION_5 = "5";

        // Index created in vES_5 - Custom-Analyzer - standard token filter
        public static final String ES_VERSION_5_STANDARD_TOKEN_FILTER = "5_standard_token_filter";

        // Index created in vES_6 - Basic mapping
        public static final String ES_VERSION_6 = "6";

        // Index created in vES_6 - Custom-Analyzer - standard token filter
        public static final String ES_VERSION_6_STANDARD_TOKEN_FILTER = "6_standard_token_filter";

        // Index created in vES_6 - upgraded to vES_7 LuceneCodec80
        public static final String ES_VERSION_6_LUCENE_CODEC_80 = "lucene_80";

        // Index created in vES_6 - upgraded to vES_7 LuceneCodec84
        public static final String ES_VERSION_6_LUCENE_CODEC_84 = "lucene_84";

        // Index created in vES_6 - upgraded to vES_7 LuceneCodec86
        public static final String ES_VERSION_6_LUCENE_CODEC_86 = "lucene_86";

        // Index created in vES_6 - upgraded to vES_7 LuceneCodec87
        public static final String ES_VERSION_6_LUCENE_CODEC_87 = "lucene_87";
    }
}
