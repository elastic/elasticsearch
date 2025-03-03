/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.test.cluster.util.Version.CURRENT;
import static org.elasticsearch.test.cluster.util.Version.fromString;
import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractIndexCompatibilityTestCase extends ESRestTestCase {

    protected static final Version VERSION_MINUS_2 = fromString(System.getProperty("tests.minimum.index.compatible"));
    protected static final Version VERSION_MINUS_1 = fromString(System.getProperty("tests.minimum.wire.compatible"));
    protected static final Version VERSION_CURRENT = CURRENT;

    protected static final int NODES = 3;

    private static TemporaryFolder REPOSITORY_PATH = new TemporaryFolder();

    protected static LocalClusterConfigProvider clusterConfig = c -> {};
    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(VERSION_MINUS_2)
        .nodes(NODES)
        .setting("path.repo", () -> REPOSITORY_PATH.getRoot().getPath())
        .setting("xpack.security.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("path.repo", () -> REPOSITORY_PATH.getRoot().getPath())
        .apply(() -> clusterConfig)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(REPOSITORY_PATH).around(cluster);

    private static boolean upgradeFailed = false;

    @Before
    public final void maybeUpgradeBeforeTest() throws Exception {
        // We want to use this test suite for the V9 upgrade, but we are not fully committed to necessarily having N-2 support
        // in V10, so we add a check here to ensure we'll revisit this decision once V10 exists.
        assertThat("Explicit check that N-2 version is Elasticsearch 7", VERSION_MINUS_2.getMajor(), equalTo(7));

        if (upgradeFailed == false) {
            try {
                maybeUpgrade();
            } catch (Exception e) {
                upgradeFailed = true;
                throw e;
            }
        }

        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);
    }

    protected abstract void maybeUpgrade() throws Exception;

    @After
    public final void deleteSnapshotBlobCache() throws IOException {
        // TODO ES-10475: The .snapshot-blob-cache created in legacy version can block upgrades, we should probably delete it automatically
        try {
            var request = new Request("DELETE", "/.snapshot-blob-cache");
            request.setOptions(
                expectWarnings(
                    "this request accesses system indices: [.snapshot-blob-cache], but in a future major version, "
                        + "direct access to system indices will be prevented by default"
                )
            );
            adminClient().performRequest(request);
        } catch (IOException e) {
            if (isNotFoundResponseException(e) == false) {
                throw e;
            }
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    protected String suffix(String name) {
        return name + '-' + getTestName().split(" ")[0].toLowerCase(Locale.ROOT);
    }

    protected Settings repositorySettings() {
        return Settings.builder()
            .put("location", REPOSITORY_PATH.getRoot().toPath().resolve(suffix("location")).toFile().getPath())
            .build();
    }

    protected static Map<String, Version> nodesVersions() throws Exception {
        var nodesInfos = getNodesInfo(adminClient());
        assertThat(nodesInfos.size(), equalTo(NODES));
        var versions = new HashMap<String, Version>();
        for (var nodeInfos : nodesInfos.values()) {
            versions.put((String) nodeInfos.get("name"), Version.fromString((String) nodeInfos.get("version")));
        }
        return versions;
    }

    protected static boolean isFullyUpgradedTo(Version version) throws Exception {
        return nodesVersions().values().stream().allMatch(v -> v.equals(version));
    }

    protected static Version indexVersion(String indexName) throws Exception {
        return indexVersion(indexName, false);
    }

    protected static Version indexVersion(String indexName, boolean ignoreWarnings) throws Exception {
        Request request = new Request("GET", "/" + indexName + "/_settings");
        request.addParameter("flat_settings", "true");
        if (ignoreWarnings) {
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.setWarningsHandler(WarningsHandler.PERMISSIVE);
            request.setOptions(options);
        }
        var response = assertOK(client().performRequest(request));
        ObjectPath fromResponse = createFromResponse(response);
        Map<String, Object> settings = fromResponse.evaluateExact(indexName, "settings");
        int id = Integer.parseInt((String) settings.get("index.version.created"));
        return new Version((byte) ((id / 1000000) % 100), (byte) ((id / 10000) % 100), (byte) ((id / 100) % 100));
    }

    protected static int getNumberOfReplicas(String indexName) throws Exception {
        var indexSettings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(indexName).get(indexName)).get("settings");
        var numberOfReplicas = Integer.parseInt((String) indexSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
        assertThat(numberOfReplicas, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(NODES - 1)));
        return numberOfReplicas;
    }

    protected static void indexDocs(String indexName, int numDocs) throws Exception {
        var request = new Request("POST", "/_bulk");
        request.addParameter("refresh", "true");
        var docs = new StringBuilder();
        IntStream.range(0, numDocs).forEach(n -> docs.append(Strings.format("""
            {"index":{"_index":"%s"}}
            {"field_0":"%s","field_1":%d,"field_2":"%s"}
            """, indexName, Integer.toString(n), n, randomFrom(Locale.getAvailableLocales()).getDisplayName())));
        request.setJsonEntity(docs.toString());
        var response = assertOK(client().performRequest(request));
        assertThat(entityAsMap(response).get("errors"), allOf(notNullValue(), is(false)));
    }

    protected static void mountIndex(String repository, String snapshot, String indexName, boolean partial, String renamedIndexName)
        throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_mount");
        request.addParameter("wait_for_completion", "true");
        var storage = partial ? "shared_cache" : "full_copy";
        request.addParameter("storage", storage);
        request.setJsonEntity(Strings.format("""
            {
              "index": "%s",
              "renamed_index": "%s"
            }""", indexName, renamedIndexName));
        var responseBody = createFromResponse(client().performRequest(request));
        assertThat(responseBody.evaluate("snapshot.shards.total"), equalTo((int) responseBody.evaluate("snapshot.shards.successful")));
        assertThat(responseBody.evaluate("snapshot.shards.failed"), equalTo(0));
    }

    protected static void restoreIndex(String repository, String snapshot, String indexName, String renamedIndexName) throws Exception {
        restoreIndex(repository, snapshot, indexName, renamedIndexName, Settings.EMPTY);
    }

    protected static void restoreIndex(
        String repository,
        String snapshot,
        String indexName,
        String renamedIndexName,
        Settings indexSettings
    ) throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(Strings.format("""
            {
              "indices": "%s",
              "include_global_state": false,
              "rename_pattern": "(.+)",
              "rename_replacement": "%s",
              "include_aliases": false,
              "index_settings": %s
            }""", indexName, renamedIndexName, Strings.toString(indexSettings)));
        var responseBody = createFromResponse(client().performRequest(request));
        assertThat(responseBody.evaluate("snapshot.shards.total"), equalTo((int) responseBody.evaluate("snapshot.shards.successful")));
        assertThat(responseBody.evaluate("snapshot.shards.failed"), equalTo(0));
    }

    protected static void updateRandomIndexSettings(String indexName) throws IOException {
        final var settings = Settings.builder();
        int updates = randomIntBetween(1, 3);
        for (int i = 0; i < updates; i++) {
            switch (i) {
                case 0 -> settings.putList(IndexSettings.DEFAULT_FIELD_SETTING.getKey(), "field_" + randomInt(2));
                case 1 -> settings.put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), randomIntBetween(1, 100));
                case 2 -> settings.put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), randomLongBetween(100L, 1000L));
                case 3 -> settings.put(IndexSettings.MAX_SLICES_PER_SCROLL.getKey(), randomIntBetween(1, 1024));
                default -> throw new IllegalStateException();
            }
        }
        updateIndexSettings(indexName, settings);
    }

    protected static void updateRandomMappings(String indexName) throws Exception {
        final var runtime = new HashMap<>();
        runtime.put("field_" + randomInt(2), Map.of("type", "keyword"));
        final var properties = new HashMap<>();
        properties.put(randomIdentifier(), Map.of("type", "long"));
        updateMappings(indexName, Map.of("runtime", runtime, "properties", properties));
    }

    protected static void updateMappings(String indexName, Map<String, ?> mappings) throws Exception {
        var body = XContentTestUtils.convertToXContent(mappings, XContentType.JSON);
        var request = new Request("PUT", indexName + "/_mappings");
        request.setEntity(
            new InputStreamEntity(body.streamInput(), body.length(), ContentType.create(XContentType.JSON.mediaTypeWithoutParameters()))
        );
        assertOK(client().performRequest(request));
    }

    protected static boolean isIndexClosed(String indexName) throws Exception {
        var responseBody = createFromResponse(client().performRequest(new Request("GET", "_cluster/state/metadata/" + indexName)));
        var state = responseBody.evaluate("metadata.indices." + indexName + ".state");
        return IndexMetadata.State.fromString((String) state) == IndexMetadata.State.CLOSE;
    }

    protected static void forceMerge(String indexName, int maxNumSegments) throws Exception {
        var request = new Request("POST", '/' + indexName + "/_forcemerge");
        request.addParameter("max_num_segments", String.valueOf(maxNumSegments));
        assertOK(client().performRequest(request));
    }

    protected void addIndexBlock(String indexName, IndexMetadata.APIBlock apiBlock) throws Exception {
        logger.debug("--> adding index block [{}] to [{}]", apiBlock, indexName);
        var request = new Request("PUT", Strings.format("/%s/_block/%s", indexName, apiBlock.name().toLowerCase(Locale.ROOT)));
        assertAcknowledged(client().performRequest(request));
    }

    private static ClusterBlock toIndexBlock(String blockId) {
        int block = Integer.parseInt(blockId);
        for (var indexBlock : List.of(
            IndexMetadata.INDEX_READ_ONLY_BLOCK,
            IndexMetadata.INDEX_READ_BLOCK,
            IndexMetadata.INDEX_WRITE_BLOCK,
            IndexMetadata.INDEX_METADATA_BLOCK,
            IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
            IndexMetadata.INDEX_REFRESH_BLOCK,
            MetadataIndexStateService.INDEX_CLOSED_BLOCK
        )) {
            if (block == indexBlock.id()) {
                return indexBlock;
            }
        }
        throw new AssertionError("No index block found with id [" + blockId + ']');
    }

    @SuppressWarnings("unchecked")
    protected static List<ClusterBlock> indexBlocks(String indexName) throws Exception {
        var responseBody = createFromResponse(client().performRequest(new Request("GET", "_cluster/state/blocks/" + indexName)));
        var blocks = (Map<String, ?>) responseBody.evaluate("blocks.indices." + indexName);
        if (blocks == null || blocks.isEmpty()) {
            return List.of();
        }
        return blocks.keySet()
            .stream()
            .map(AbstractIndexCompatibilityTestCase::toIndexBlock)
            .sorted(Comparator.comparing(ClusterBlock::id))
            .toList();
    }

    @SuppressWarnings("unchecked")
    protected static void assertIndexSetting(String indexName, Setting<?> setting, Matcher<Boolean> matcher) throws Exception {
        var indexSettings = getIndexSettingsAsMap(indexName);
        assertThat(Boolean.parseBoolean((String) indexSettings.get(setting.getKey())), matcher);
    }

    protected static ResponseException expectUpdateIndexSettingsThrows(String indexName, Settings.Builder settings) {
        var exception = expectThrows(ResponseException.class, () -> updateIndexSettings(indexName, settings));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        return exception;
    }

    protected static Matcher<String> containsStringCannotRemoveBlockOnReadOnlyIndex(String indexName) {
        return allOf(containsString("Can't remove the write block on read-only compatible index"), containsString(indexName));
    }
}
