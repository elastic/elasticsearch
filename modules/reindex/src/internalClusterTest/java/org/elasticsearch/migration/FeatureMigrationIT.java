/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.migration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusAction;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusRequest;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeAction;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeRequest;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.AssociatedIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.upgrades.FeatureMigrationResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FeatureMigrationIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // We need to be able to set the index creation version manually.
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    public void testStartMigrationAndImmediatelyCheckStatus() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_MANAGED);
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);
        createSystemIndexForDescriptor(EXTERNAL_MANAGED);
        createSystemIndexForDescriptor(EXTERNAL_UNMANAGED);

        TestPlugin.preMigrationHook.set((state) -> Collections.emptyMap());
        TestPlugin.postMigrationHook.set((state, metadata) -> {});

        ensureGreen();

        PostFeatureUpgradeRequest migrationRequest = new PostFeatureUpgradeRequest();
        GetFeatureUpgradeStatusRequest getStatusRequest = new GetFeatureUpgradeStatusRequest();

        // Start the migration and *immediately* request the status. We're trying to detect a race condition with this test, so we need to
        // do this as fast as possible, but not before the request to start the migration completes.
        PostFeatureUpgradeResponse migrationResponse = client().execute(PostFeatureUpgradeAction.INSTANCE, migrationRequest).get();
        GetFeatureUpgradeStatusResponse statusResponse = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest).get();

        // Make sure we actually started the migration
        final Set<String> migratingFeatures = migrationResponse.getFeatures()
            .stream()
            .map(PostFeatureUpgradeResponse.Feature::getFeatureName)
            .collect(Collectors.toSet());
        assertThat(migratingFeatures, hasItem(FEATURE_NAME));

        // We should see that the migration is in progress even though we just started the migration.
        assertThat(statusResponse.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.IN_PROGRESS));

        // Now wait for the migration to finish (otherwise the test infra explodes)
        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }

    public void testMigrateInternalManagedSystemIndex() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_MANAGED);
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);
        createSystemIndexForDescriptor(EXTERNAL_MANAGED);
        createSystemIndexForDescriptor(EXTERNAL_UNMANAGED);

        CreateIndexRequestBuilder createRequest = prepareCreate(ASSOCIATED_INDEX_NAME);
        createRequest.setWaitForActiveShards(ActiveShardCount.ALL);
        createRequest.setSettings(
            Settings.builder()
                .put("index.version.created", NEEDS_UPGRADE_VERSION)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                .put("index.hidden", true) // So we don't get a warning
                .build()
        );
        CreateIndexResponse response = createRequest.get();
        assertTrue(response.isShardsAcknowledged());

        ensureGreen();

        SetOnce<Boolean> preUpgradeHookCalled = new SetOnce<>();
        SetOnce<Boolean> postUpgradeHookCalled = new SetOnce<>();
        TestPlugin.preMigrationHook.set(clusterState -> {
            // Check that the ordering of these calls is correct.
            assertThat(postUpgradeHookCalled.get(), nullValue());
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("stringKey", "stringValue");
            metadata.put("intKey", 42);
            {
                Map<String, Object> innerMetadata = new HashMap<>();
                innerMetadata.put("innerKey", "innerValue");

                metadata.put("mapKey", innerMetadata);
            }
            metadata.put("listKey", Arrays.asList(1, 2, 3, 4));
            preUpgradeHookCalled.set(true);
            return metadata;
        });

        TestPlugin.postMigrationHook.set((clusterState, metadata) -> {
            assertThat(preUpgradeHookCalled.get(), is(true));

            assertThat(metadata, hasEntry("stringKey", "stringValue"));
            assertThat(metadata, hasEntry("intKey", 42));
            assertThat(metadata, hasEntry("listKey", Arrays.asList(1, 2, 3, 4)));
            assertThat(metadata, hasKey("mapKey"));
            @SuppressWarnings("unchecked")
            Map<String, Object> innerMap = (Map<String, Object>) metadata.get("mapKey");
            assertThat(innerMap, hasEntry("innerKey", "innerValue"));

            // We shouldn't have any results in the cluster state as no features have fully finished yet.
            FeatureMigrationResults currentResults = clusterState.metadata().custom(FeatureMigrationResults.TYPE);
            assertThat(currentResults, nullValue());
            postUpgradeHookCalled.set(true);
        });

        PostFeatureUpgradeRequest migrationRequest = new PostFeatureUpgradeRequest();
        PostFeatureUpgradeResponse migrationResponse = client().execute(PostFeatureUpgradeAction.INSTANCE, migrationRequest).get();
        assertThat(migrationResponse.getReason(), nullValue());
        assertThat(migrationResponse.getElasticsearchException(), nullValue());
        final Set<String> migratingFeatures = migrationResponse.getFeatures()
            .stream()
            .map(PostFeatureUpgradeResponse.Feature::getFeatureName)
            .collect(Collectors.toSet());
        assertThat(migratingFeatures, hasItem(FEATURE_NAME));

        GetFeatureUpgradeStatusRequest getStatusRequest = new GetFeatureUpgradeStatusRequest();
        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResponse = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest)
                .get();
            logger.info(Strings.toString(statusResponse));
            assertThat(statusResponse.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });

        assertTrue("the pre-migration hook wasn't actually called", preUpgradeHookCalled.get());
        assertTrue("the post-migration hook wasn't actually called", postUpgradeHookCalled.get());

        Metadata finalMetadata = client().admin().cluster().prepareState().get().getState().metadata();
        // Check that the results metadata is what we expect.
        FeatureMigrationResults currentResults = finalMetadata.custom(FeatureMigrationResults.TYPE);
        assertThat(currentResults, notNullValue());
        assertThat(currentResults.getFeatureStatuses(), allOf(aMapWithSize(1), hasKey(FEATURE_NAME)));
        assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).succeeded(), is(true));
        assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getFailedIndexName(), nullValue());
        assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getException(), nullValue());

        assertIndexHasCorrectProperties(
            finalMetadata,
            ".int-man-old-reindexed-for-8",
            INTERNAL_MANAGED_FLAG_VALUE,
            true,
            true,
            Arrays.asList(".int-man-old", ".internal-managed-alias")
        );
        assertIndexHasCorrectProperties(
            finalMetadata,
            ".int-unman-old-reindexed-for-8",
            INTERNAL_UNMANAGED_FLAG_VALUE,
            false,
            true,
            Collections.singletonList(".int-unman-old")
        );
        assertIndexHasCorrectProperties(
            finalMetadata,
            ".ext-man-old-reindexed-for-8",
            EXTERNAL_MANAGED_FLAG_VALUE,
            true,
            false,
            Arrays.asList(".ext-man-old", ".external-managed-alias")
        );
        assertIndexHasCorrectProperties(
            finalMetadata,
            ".ext-unman-old-reindexed-for-8",
            EXTERNAL_UNMANAGED_FLAG_VALUE,
            false,
            false,
            Collections.singletonList(".ext-unman-old")
        );
    }

    public void testMigrateIndexWithWriteBlock() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);

        String indexName = Optional.ofNullable(INTERNAL_UNMANAGED.getPrimaryIndex())
            .orElse(INTERNAL_UNMANAGED.getIndexPattern().replace("*", "old"));
        client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("index.blocks.write", true)).get();

        TestPlugin.preMigrationHook.set((state) -> Collections.emptyMap());
        TestPlugin.postMigrationHook.set((state, metadata) -> {});

        ensureGreen();

        client().execute(PostFeatureUpgradeAction.INSTANCE, new PostFeatureUpgradeRequest()).get();

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest()
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }

    public void assertIndexHasCorrectProperties(
        Metadata metadata,
        String indexName,
        int settingsFlagValue,
        boolean isManaged,
        boolean isInternal,
        Collection<String> aliasNames
    ) {
        IndexMetadata imd = metadata.index(indexName);
        assertThat(imd.getSettings().get(FlAG_SETTING_KEY), equalTo(Integer.toString(settingsFlagValue)));
        final Map<String, Object> mapping = imd.mapping().getSourceAsMap();
        @SuppressWarnings("unchecked")
        final Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        assertThat(meta.get(DESCRIPTOR_MANAGED_META_KEY), is(isManaged));
        assertThat(meta.get(DESCRIPTOR_INTERNAL_META_KEY), is(isInternal));

        assertThat(imd.isSystem(), is(true));

        Set<String> actualAliasNames = imd.getAliases().keySet();
        assertThat(actualAliasNames, containsInAnyOrder(aliasNames.toArray()));

        IndicesStatsResponse indexStats = client().admin().indices().prepareStats(imd.getIndex().getName()).setDocs(true).get();
        assertNotNull(indexStats);
        final IndexStats thisIndexStats = indexStats.getIndex(imd.getIndex().getName());
        assertNotNull(thisIndexStats);
        assertNotNull(thisIndexStats.getTotal());
        assertNotNull(thisIndexStats.getTotal().getDocs());
        assertThat(thisIndexStats.getTotal().getDocs().getCount(), is((long) INDEX_DOC_COUNT));
    }

    public void createSystemIndexForDescriptor(SystemIndexDescriptor descriptor) throws InterruptedException {
        assertTrue(
            "the strategy used below to create index names for descriptors without a primary index name only works for simple patterns",
            descriptor.getIndexPattern().endsWith("*")
        );
        String indexName = Optional.ofNullable(descriptor.getPrimaryIndex()).orElse(descriptor.getIndexPattern().replace("*", "old"));
        CreateIndexRequestBuilder createRequest = prepareCreate(indexName);
        createRequest.setWaitForActiveShards(ActiveShardCount.ALL);
        if (SystemIndexDescriptor.DEFAULT_SETTINGS.equals(descriptor.getSettings())) {
            // unmanaged
            createRequest.setSettings(
                createSimpleSettings(
                    NEEDS_UPGRADE_VERSION,
                    descriptor.isInternal() ? INTERNAL_UNMANAGED_FLAG_VALUE : EXTERNAL_UNMANAGED_FLAG_VALUE
                )
            );
        } else {
            // managed
            createRequest.setSettings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .build()
            );
        }
        if (descriptor.getMappings() == null) {
            createRequest.setMapping(createSimpleMapping(false, descriptor.isInternal()));
        }
        CreateIndexResponse response = createRequest.get();
        assertTrue(response.isShardsAcknowledged());

        List<IndexRequestBuilder> docs = new ArrayList<>(INDEX_DOC_COUNT);
        for (int i = 0; i < INDEX_DOC_COUNT; i++) {
            docs.add(client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("some_field", "words words"));
        }
        indexRandom(true, docs);
        IndicesStatsResponse indexStats = client().admin().indices().prepareStats(indexName).setDocs(true).get();
        assertThat(indexStats.getIndex(indexName).getTotal().getDocs().getCount(), is((long) INDEX_DOC_COUNT));
    }

    static final String VERSION_META_KEY = "version";
    static final Version META_VERSION = Version.CURRENT;
    static final String DESCRIPTOR_MANAGED_META_KEY = "desciptor_managed";
    static final String DESCRIPTOR_INTERNAL_META_KEY = "descriptor_internal";
    static final String FEATURE_NAME = "A-test-feature"; // Sorts alphabetically before the feature from MultiFeatureMigrationIT
    static final String ORIGIN = FeatureMigrationIT.class.getSimpleName();
    static final String FlAG_SETTING_KEY = IndexMetadata.INDEX_PRIORITY_SETTING.getKey();
    static final int INDEX_DOC_COUNT = 100; // arbitrarily chosen
    public static final Version NEEDS_UPGRADE_VERSION = Version.V_7_0_0;

    static final int INTERNAL_MANAGED_FLAG_VALUE = 1;
    static final int INTERNAL_UNMANAGED_FLAG_VALUE = 2;
    static final int EXTERNAL_MANAGED_FLAG_VALUE = 3;
    static final int EXTERNAL_UNMANAGED_FLAG_VALUE = 4;
    static final SystemIndexDescriptor INTERNAL_MANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".int-man-*")
        .setAliasName(".internal-managed-alias")
        .setPrimaryIndex(".int-man-old")
        .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
        .setSettings(createSimpleSettings(NEEDS_UPGRADE_VERSION, INTERNAL_MANAGED_FLAG_VALUE))
        .setMappings(createSimpleMapping(true, true))
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setMinimumNodeVersion(NEEDS_UPGRADE_VERSION)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    static final SystemIndexDescriptor INTERNAL_UNMANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".int-unman-*")
        .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setMinimumNodeVersion(NEEDS_UPGRADE_VERSION)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    static final SystemIndexDescriptor EXTERNAL_MANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".ext-man-*")
        .setAliasName(".external-managed-alias")
        .setPrimaryIndex(".ext-man-old")
        .setType(SystemIndexDescriptor.Type.EXTERNAL_MANAGED)
        .setSettings(createSimpleSettings(NEEDS_UPGRADE_VERSION, EXTERNAL_MANAGED_FLAG_VALUE))
        .setMappings(createSimpleMapping(true, false))
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.singletonList(ORIGIN))
        .setMinimumNodeVersion(NEEDS_UPGRADE_VERSION)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    static final SystemIndexDescriptor EXTERNAL_UNMANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".ext-unman-*")
        .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.singletonList(ORIGIN))
        .setMinimumNodeVersion(NEEDS_UPGRADE_VERSION)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    static final String ASSOCIATED_INDEX_NAME = ".my-associated-idx";

    static Settings createSimpleSettings(Version creationVersion, int flagSettingValue) {
        return Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(FlAG_SETTING_KEY, flagSettingValue)
            .put("index.version.created", creationVersion)
            .build();
    }

    static String createSimpleMapping(boolean descriptorManaged, boolean descriptorInternal) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(VERSION_META_KEY, META_VERSION);
                builder.field(DESCRIPTOR_MANAGED_META_KEY, descriptorManaged);
                builder.field(DESCRIPTOR_INTERNAL_META_KEY, descriptorInternal);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("some_field");
                    builder.field("type", "keyword");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            // Just rethrow, it should be impossible for this to throw here
            throw new AssertionError(e);
        }
    }

    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        public static final AtomicReference<Function<ClusterState, Map<String, Object>>> preMigrationHook = new AtomicReference<>();
        public static final AtomicReference<BiConsumer<ClusterState, Map<String, Object>>> postMigrationHook = new AtomicReference<>();

        public TestPlugin() {

        }

        @Override
        public String getFeatureName() {
            return FEATURE_NAME;
        }

        @Override
        public String getFeatureDescription() {
            return "a plugin for testing system index migration";
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Arrays.asList(INTERNAL_MANAGED, INTERNAL_UNMANAGED, EXTERNAL_MANAGED, EXTERNAL_UNMANAGED);
        }

        @Override
        public Collection<AssociatedIndexDescriptor> getAssociatedIndexDescriptors() {

            return Collections.singletonList(new AssociatedIndexDescriptor(ASSOCIATED_INDEX_NAME, TestPlugin.class.getCanonicalName()));
        }

        @Override
        public void prepareForIndicesMigration(ClusterService clusterService, Client client, ActionListener<Map<String, Object>> listener) {
            listener.onResponse(preMigrationHook.get().apply(clusterService.state()));
        }

        @Override
        public void indicesMigrationComplete(
            Map<String, Object> preUpgradeMetadata,
            ClusterService clusterService,
            Client client,
            ActionListener<Boolean> listener
        ) {
            postMigrationHook.get().accept(clusterService.state(), preUpgradeMetadata);
            listener.onResponse(true);
        }
    }
}
