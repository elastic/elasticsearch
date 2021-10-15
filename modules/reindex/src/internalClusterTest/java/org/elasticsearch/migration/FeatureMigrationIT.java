/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.migration;

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
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
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

    public void testMigrateInternalManagedSystemIndex() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_MANAGED);
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);
        createSystemIndexForDescriptor(EXTERNAL_MANAGED);
        createSystemIndexForDescriptor(EXTERNAL_UNMANAGED);

        ensureGreen();

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
            assertThat(statusResponse.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_UPGRADE_NEEDED));
        });

        Metadata finalMetadata = client().admin().cluster().prepareState().get().getState().metadata();
        assertIndexHasCorrectProperties(finalMetadata, ".int-man-old-reindexed-for-8", INTERNAL_MANAGED_FLAG_VALUE, true, true);
        assertIndexHasCorrectProperties(finalMetadata, ".int-unman-old-reindexed-for-8", INTERNAL_UNMANAGED_FLAG_VALUE, false, true);
        assertIndexHasCorrectProperties(finalMetadata, ".ext-man-old-reindexed-for-8", EXTERNAL_MANAGED_FLAG_VALUE, true, false);
        assertIndexHasCorrectProperties(finalMetadata, ".ext-unman-old-reindexed-for-8", EXTERNAL_UNMANAGED_FLAG_VALUE, false, false);

    }

    public void assertIndexHasCorrectProperties(
        Metadata metadata,
        String indexName,
        int settingsFlagValue,
        boolean isManaged,
        boolean isInternal
    ) {
        IndexMetadata imd = metadata.index(indexName);
        assertThat(imd.getSettings().get(FlAG_SETTING_KEY), equalTo(Integer.toString(settingsFlagValue)));
        final Map<String, Object> mapping = imd.mapping().getSourceAsMap();
        @SuppressWarnings("unchecked")
        final Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        assertThat(meta.get(DESCRIPTOR_MANAGED_META_KEY), is(isManaged));
        assertThat(meta.get(DESCRIPTOR_INTERNAL_META_KEY), is(isInternal));

        IndicesStatsResponse indexStats = client().admin().indices().prepareStats(imd.getIndex().getName()).setDocs(true).get();
        assertThat(indexStats.getIndex(imd.getIndex().getName()).getTotal().getDocs().getCount(), is(INDEX_DOC_COUNT));
    }

    public void createSystemIndexForDescriptor(SystemIndexDescriptor descriptor) throws InterruptedException {
        assertTrue(
            "the strategy used below to create index names for descriptors without a primary index name only works for simple patterns",
            descriptor.getIndexPattern().endsWith("*")
        );
        String indexName = Optional.ofNullable(descriptor.getPrimaryIndex()).orElse(descriptor.getIndexPattern().replace("*", "old"));
        CreateIndexRequestBuilder createRequest = prepareCreate(indexName);
        createRequest.setWaitForActiveShards(ActiveShardCount.ALL);
        if (descriptor.getAliasName() != null) {
            // createRequest.addAlias(new Alias(descriptor.getAliasName()));
        }
        if (descriptor.getSettings() != null) {
            // createRequest.setSettings(descriptor.getSettings());
            createRequest.setSettings(Settings.builder().put("index.version.created", Version.CURRENT).build());
        } else {
            createRequest.setSettings(
                createSimpleSettings(
                    Version.V_7_0_0,
                    descriptor.isInternal() ? INTERNAL_UNMANAGED_FLAG_VALUE : EXTERNAL_UNMANAGED_FLAG_VALUE
                )
            );
        }
        if (descriptor.getMappings() != null) {
            // createRequest.setMapping(descriptor.getMappings());
        } else {
            createRequest.setMapping(createSimpleMapping(false, descriptor.isInternal()));
        }
        CreateIndexResponse response = createRequest.get();
        assertTrue(response.isShardsAcknowledged());

        List<IndexRequestBuilder> docs = new ArrayList<>(INDEX_DOC_COUNT);
        for (int i = 0; i < INDEX_DOC_COUNT; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("some_field", "words words"));
        }
        indexRandom(true, docs);
    }

    private static final String VERSION_META_KEY = "version";
    private static final Version META_VERSION = Version.CURRENT;
    private static final String DESCRIPTOR_MANAGED_META_KEY = "desciptor_managed";
    private static final String DESCRIPTOR_INTERNAL_META_KEY = "descriptor_internal";
    private static final String FEATURE_NAME = FeatureMigrationIT.class.getSimpleName();
    private static final String ORIGIN = FeatureMigrationIT.class.getSimpleName();
    private static final String FlAG_SETTING_KEY = IndexMetadata.INDEX_PRIORITY_SETTING.getKey();
    private static final int INDEX_DOC_COUNT = 100; // arbitrarily chosen

    private static final int INTERNAL_MANAGED_FLAG_VALUE = 1;
    private static final int INTERNAL_UNMANAGED_FLAG_VALUE = 2;
    private static final int EXTERNAL_MANAGED_FLAG_VALUE = 3;
    private static final int EXTERNAL_UNMANAGED_FLAG_VALUE = 4;
    private static final SystemIndexDescriptor INTERNAL_MANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".int-man-*")
        .setAliasName(".internal-managed-alias")
        .setPrimaryIndex(".int-man-old")
        .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
        .setSettings(createSimpleSettings(Version.V_7_0_0, INTERNAL_MANAGED_FLAG_VALUE))
        .setMappings(createSimpleMapping(true, true))
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setMinimumNodeVersion(Version.V_7_0_0)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    private static final SystemIndexDescriptor INTERNAL_UNMANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".int-unman-*")
        .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setMinimumNodeVersion(Version.V_7_0_0)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    private static final SystemIndexDescriptor EXTERNAL_MANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".ext-man-*")
        .setAliasName(".external-managed-alias")
        .setPrimaryIndex(".ext-man-old")
        .setType(SystemIndexDescriptor.Type.EXTERNAL_MANAGED)
        .setSettings(createSimpleSettings(Version.V_7_0_0, EXTERNAL_MANAGED_FLAG_VALUE))
        .setMappings(createSimpleMapping(true, false))
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.singletonList(ORIGIN))
        .setMinimumNodeVersion(Version.V_7_0_0)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    private static final SystemIndexDescriptor EXTERNAL_UNMANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".ext-unman-*")
        .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
        .setOrigin(ORIGIN)
        .setVersionMetaKey(VERSION_META_KEY)
        .setAllowedElasticProductOrigins(Collections.singletonList(ORIGIN))
        .setMinimumNodeVersion(Version.V_7_0_0)
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();

    private static Settings createSimpleSettings(Version creationVersion, int flagSettingValue) {
        return Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(FlAG_SETTING_KEY, flagSettingValue)
            .put("index.version.created", creationVersion)
            .build();
    }

    private static String createSimpleMapping(boolean descriptorManaged, boolean descriptorInternal) {
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

        private AtomicReference<Function<ClusterState, Map<String, Object>>> preMigrationHook;
        private AtomicReference<BiConsumer<ClusterState, Map<String, Object>>> postMigrationHook;

        public TestPlugin() {

        }

        public void setPreMigrationHook(Function<ClusterState, Map<String, Object>> preMigrationHook) {
            this.preMigrationHook.set(preMigrationHook);
        }

        public void setPostMigrationHook(BiConsumer<ClusterState, Map<String, Object>> postMigrationHook) {
            this.postMigrationHook.set(postMigrationHook);
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
        public void prepareForIndicesMigration(ClusterService clusterService, Client client, ActionListener<Map<String, Object>> listener) {
            listener.onResponse(this.preMigrationHook.get().apply(clusterService.state()));
        }

        @Override
        public void indicesMigrationComplete(
            Map<String, Object> preUpgradeMetadata,
            ClusterService clusterService,
            Client client,
            ActionListener<Boolean> listener
        ) {
            this.postMigrationHook.get().accept(clusterService.state(), preUpgradeMetadata);
            listener.onResponse(true);
        }
    }
}
