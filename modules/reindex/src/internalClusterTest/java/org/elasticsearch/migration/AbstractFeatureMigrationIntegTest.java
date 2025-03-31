/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.migration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.migration.TransportGetFeatureUpgradeStatusAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.AssociatedIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public abstract class AbstractFeatureMigrationIntegTest extends ESIntegTestCase {

    static final String VERSION_META_KEY = "version";
    static final Version META_VERSION = Version.CURRENT;
    static final String DESCRIPTOR_MANAGED_META_KEY = "desciptor_managed";
    static final String DESCRIPTOR_INTERNAL_META_KEY = "descriptor_internal";
    static final String FEATURE_NAME = "A-test-feature"; // Sorts alphabetically before the feature from MultiFeatureMigrationIT
    static final String ORIGIN = AbstractFeatureMigrationIntegTest.class.getSimpleName();
    static final String FlAG_SETTING_KEY = IndexMetadata.INDEX_PRIORITY_SETTING.getKey();
    static final String INTERNAL_MANAGED_INDEX_NAME = ".int-man-old";
    static final int INDEX_DOC_COUNT = 100; // arbitrarily chosen
    static final int INTERNAL_MANAGED_FLAG_VALUE = 1;
    static final String FIELD_NAME = "some_field";
    protected static final IndexVersion NEEDS_UPGRADE_INDEX_VERSION = IndexVersionUtils.getPreviousMajorVersion(
        TransportGetFeatureUpgradeStatusAction.NO_UPGRADE_REQUIRED_INDEX_VERSION
    );
    protected static final int UPGRADED_TO_VERSION = TransportGetFeatureUpgradeStatusAction.NO_UPGRADE_REQUIRED_VERSION.major + 1;

    static final SystemIndexDescriptor EXTERNAL_UNMANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".ext-unman-*")
        .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
        .setOrigin(ORIGIN)
        .setAllowedElasticProductOrigins(Collections.singletonList(ORIGIN))
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    static final SystemIndexDescriptor INTERNAL_UNMANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".int-unman-*")
        .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
        .setOrigin(ORIGIN)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();

    static final SystemIndexDescriptor INTERNAL_MANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".int-man-*")
        .setAliasName(".internal-managed-alias")
        .setPrimaryIndex(INTERNAL_MANAGED_INDEX_NAME)
        .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
        .setSettings(createSettings(NEEDS_UPGRADE_INDEX_VERSION, INTERNAL_MANAGED_FLAG_VALUE))
        .setMappings(createMapping(true, true))
        .setOrigin(ORIGIN)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    static final int INTERNAL_UNMANAGED_FLAG_VALUE = 2;
    static final int EXTERNAL_MANAGED_FLAG_VALUE = 3;
    static final SystemIndexDescriptor EXTERNAL_MANAGED = SystemIndexDescriptor.builder()
        .setIndexPattern(".ext-man-*")
        .setAliasName(".external-managed-alias")
        .setPrimaryIndex(".ext-man-old")
        .setType(SystemIndexDescriptor.Type.EXTERNAL_MANAGED)
        .setSettings(createSettings(NEEDS_UPGRADE_INDEX_VERSION, EXTERNAL_MANAGED_FLAG_VALUE))
        .setMappings(createMapping(true, false))
        .setOrigin(ORIGIN)
        .setAllowedElasticProductOrigins(Collections.singletonList(ORIGIN))
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();
    static final int EXTERNAL_UNMANAGED_FLAG_VALUE = 4;
    static final String ASSOCIATED_INDEX_NAME = ".my-associated-idx";

    public static final SystemIndexDescriptor KIBANA_MOCK_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".kibana_*")
        .setDescription("Kibana saved objects system index")
        .setAliasName(".kibana")
        .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setAllowedElasticProductOrigins(Collections.singletonList(ORIGIN))
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .setAllowsTemplates()
        .build();

    protected String masterAndDataNode;
    protected String masterName;

    @Before
    public void setup() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        masterName = internalCluster().startMasterOnlyNode();
        masterAndDataNode = internalCluster().startNode();

        TestPlugin testPlugin = getPlugin(TestPlugin.class);
        testPlugin.preMigrationHook.set((state) -> Collections.emptyMap());
        testPlugin.postMigrationHook.set((state, metadata) -> {});
    }

    public <T extends Plugin> T getPlugin(Class<T> type) {
        final PluginsService pluginsService = internalCluster().getCurrentMasterNodeInstance(PluginsService.class);
        return pluginsService.filterPlugins(type).findFirst().get();
    }

    protected void createSystemIndexForDescriptor(SystemIndexDescriptor descriptor) {
        assertThat(
            "the strategy used below to create index names for descriptors without a primary index name only works for simple patterns",
            descriptor.getIndexPattern(),
            endsWith("*")
        );
        String indexName = descriptor.isAutomaticallyManaged()
            ? descriptor.getPrimaryIndex()
            : descriptor.getIndexPattern().replace("*", "old");
        CreateIndexRequestBuilder createRequest = prepareCreate(indexName);
        createRequest.setWaitForActiveShards(ActiveShardCount.ALL);
        if (descriptor.isAutomaticallyManaged() == false || SystemIndexDescriptor.DEFAULT_SETTINGS.equals(descriptor.getSettings())) {
            // unmanaged
            createRequest.setSettings(
                createSettings(
                    NEEDS_UPGRADE_INDEX_VERSION,
                    descriptor.isInternal() ? INTERNAL_UNMANAGED_FLAG_VALUE : EXTERNAL_UNMANAGED_FLAG_VALUE
                )
            );
        } else {
            // managed
            createRequest.setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .build()
            );
        }
        if (descriptor.isAutomaticallyManaged() == false) {
            createRequest.setMapping(createMapping(false, descriptor.isInternal()));
        }
        CreateIndexResponse response = createRequest.get();
        Assert.assertTrue(response.isShardsAcknowledged());

        indexDocs(indexName);
    }

    protected void indexDocs(String indexName) {
        List<IndexRequestBuilder> docs = new ArrayList<>(INDEX_DOC_COUNT);
        for (int i = 0; i < INDEX_DOC_COUNT; i++) {
            docs.add(ESIntegTestCase.prepareIndex(indexName).setId(Integer.toString(i)).setSource(FIELD_NAME, "words words"));
        }
        indexRandom(true, docs);
        IndicesStatsResponse indexStats = ESIntegTestCase.indicesAdmin().prepareStats(indexName).setDocs(true).get();
        Assert.assertThat(indexStats.getIndex(indexName).getTotal().getDocs().getCount(), is((long) INDEX_DOC_COUNT));
    }

    static Settings createSettings(IndexVersion creationVersion, int flagSettingValue) {
        return indexSettings(creationVersion, 1, 0).put(FlAG_SETTING_KEY, flagSettingValue).build();
    }

    static String createMapping(boolean descriptorManaged, boolean descriptorInternal) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(VERSION_META_KEY, META_VERSION);
                builder.field(SystemIndexDescriptor.VERSION_META_KEY, 1);
                builder.field(DESCRIPTOR_MANAGED_META_KEY, descriptorManaged);
                builder.field(DESCRIPTOR_INTERNAL_META_KEY, descriptorInternal);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject(FIELD_NAME);
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

    protected void assertIndexHasCorrectProperties(
        Metadata metadata,
        String indexName,
        int settingsFlagValue,
        boolean isManaged,
        boolean isInternal,
        Collection<String> aliasNames
    ) {
        IndexMetadata imd = metadata.getProject().index(indexName);
        assertThat(imd.getSettings().get(FlAG_SETTING_KEY), equalTo(Integer.toString(settingsFlagValue)));
        final Map<String, Object> mapping = imd.mapping().getSourceAsMap();
        @SuppressWarnings("unchecked")
        final Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        assertThat(meta.get(DESCRIPTOR_MANAGED_META_KEY), is(isManaged));
        assertThat(meta.get(DESCRIPTOR_INTERNAL_META_KEY), is(isInternal));

        assertThat(imd.isSystem(), is(true));

        Set<String> actualAliasNames = imd.getAliases().keySet();
        assertThat(actualAliasNames, containsInAnyOrder(aliasNames.toArray()));

        IndicesStatsResponse indexStats = indicesAdmin().prepareStats(imd.getIndex().getName()).setDocs(true).get();
        assertNotNull(indexStats);
        final IndexStats thisIndexStats = indexStats.getIndex(imd.getIndex().getName());
        assertNotNull(thisIndexStats);
        assertNotNull(thisIndexStats.getTotal());
        assertNotNull(thisIndexStats.getTotal().getDocs());
        assertThat(thisIndexStats.getTotal().getDocs().getCount(), is((long) INDEX_DOC_COUNT));
    }

    public static class TestPlugin extends Plugin implements SystemIndexPlugin, ActionPlugin {
        public final AtomicReference<Function<ClusterState, Map<String, Object>>> preMigrationHook = new AtomicReference<>();
        public final AtomicReference<BiConsumer<ClusterState, Map<String, Object>>> postMigrationHook = new AtomicReference<>();
        private final BlockingActionFilter blockingActionFilter;

        public TestPlugin() {
            blockingActionFilter = new BlockingActionFilter();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(blockingActionFilter);
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
            return Arrays.asList(INTERNAL_MANAGED, INTERNAL_UNMANAGED, EXTERNAL_MANAGED, EXTERNAL_UNMANAGED, KIBANA_MOCK_INDEX_DESCRIPTOR);
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

        public static class BlockingActionFilter extends org.elasticsearch.action.support.ActionFilter.Simple {
            private Set<String> blockedActions = emptySet();

            @Override
            protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
                if (blockedActions.contains(action)) {
                    throw new ElasticsearchException("force exception on [" + action + "]");
                }
                return true;
            }

            @Override
            public int order() {
                return 0;
            }

            public void blockActions(String... actions) {
                blockedActions = unmodifiableSet(newHashSet(actions));
            }
        }
    }
}
