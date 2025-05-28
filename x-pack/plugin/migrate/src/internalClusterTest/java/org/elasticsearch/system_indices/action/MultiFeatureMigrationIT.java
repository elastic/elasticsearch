/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.system_indices.task.FeatureMigrationResults;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.SystemIndices.UPGRADED_INDEX_SUFFIX;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MultiFeatureMigrationIT extends AbstractFeatureMigrationIntegTest {

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
        plugins.add(SecondPlugin.class);
        return plugins;
    }

    // Sorts alphabetically after the feature from MultiFeatureMigrationIT
    private static final String SECOND_FEATURE_NAME = "B-test-feature";
    private static final String ORIGIN = MultiFeatureMigrationIT.class.getSimpleName();
    private static final String VERSION_META_KEY = "version";
    static final int SECOND_FEATURE_IDX_FLAG_VALUE = 0;

    public void testMultipleFeatureMigration() throws Exception {
        // All the indices from FeatureMigrationIT
        createSystemIndexForDescriptor(INTERNAL_MANAGED);
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);
        createSystemIndexForDescriptor(EXTERNAL_MANAGED);
        createSystemIndexForDescriptor(EXTERNAL_UNMANAGED);
        // And our new one
        createSystemIndexForDescriptor(SECOND_FEATURE_IDX_DESCIPTOR);

        ensureGreen();

        CountDownLatch hooksCalled = new CountDownLatch(4);

        SetOnce<Boolean> preMigrationHookCalled = new SetOnce<>();
        SetOnce<Boolean> postMigrationHookCalled = new SetOnce<>();
        SetOnce<Boolean> secondPluginPreMigrationHookCalled = new SetOnce<>();
        SetOnce<Boolean> secondPluginPostMigrationHookCalled = new SetOnce<>();

        getPlugin(TestPlugin.class).preMigrationHook.set(clusterState -> {
            // None of the other hooks should have been called yet.
            assertThat(postMigrationHookCalled.get(), nullValue());
            assertThat(secondPluginPreMigrationHookCalled.get(), nullValue());
            assertThat(secondPluginPostMigrationHookCalled.get(), nullValue());
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("stringKey", "first plugin value");

            // We shouldn't have any results in the cluster state given no features have finished yet.
            FeatureMigrationResults currentResults = clusterState.metadata().getProject().custom(FeatureMigrationResults.TYPE);
            assertThat(currentResults, nullValue());

            preMigrationHookCalled.set(true);
            hooksCalled.countDown();
            return metadata;
        });

        getPlugin(TestPlugin.class).postMigrationHook.set((clusterState, metadata) -> {
            // Check that the hooks have been called or not as expected.
            assertThat(preMigrationHookCalled.get(), is(true));
            assertThat(secondPluginPreMigrationHookCalled.get(), nullValue());
            assertThat(secondPluginPostMigrationHookCalled.get(), nullValue());

            assertThat(metadata, hasEntry("stringKey", "first plugin value"));

            // We shouldn't have any results in the cluster state given no features have finished yet.
            FeatureMigrationResults currentResults = clusterState.metadata().getProject().custom(FeatureMigrationResults.TYPE);
            assertThat(currentResults, nullValue());

            postMigrationHookCalled.set(true);
            hooksCalled.countDown();
        });

        getPlugin(SecondPlugin.class).preMigrationHook.set(clusterState -> {
            // Check that the hooks have been called or not as expected.
            assertThat(preMigrationHookCalled.get(), is(true));
            assertThat(postMigrationHookCalled.get(), is(true));
            assertThat(secondPluginPostMigrationHookCalled.get(), nullValue());

            Map<String, Object> metadata = new HashMap<>();
            metadata.put("stringKey", "second plugin value");

            // But now, we should have results, as we're in a new feature!
            FeatureMigrationResults currentResults = clusterState.metadata().getProject().custom(FeatureMigrationResults.TYPE);
            assertThat(currentResults, notNullValue());
            assertThat(currentResults.getFeatureStatuses(), allOf(aMapWithSize(1), hasKey(FEATURE_NAME)));
            assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).succeeded(), is(true));
            assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getFailedResourceName(), nullValue());
            assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getException(), nullValue());

            secondPluginPreMigrationHookCalled.set(true);
            hooksCalled.countDown();
            return metadata;
        });

        getPlugin(SecondPlugin.class).postMigrationHook.set((clusterState, metadata) -> {
            // Check that the hooks have been called or not as expected.
            assertThat(preMigrationHookCalled.get(), is(true));
            assertThat(postMigrationHookCalled.get(), is(true));
            assertThat(secondPluginPreMigrationHookCalled.get(), is(true));

            assertThat(metadata, hasEntry("stringKey", "second plugin value"));

            // And here, the results should be the same, as we haven't updated the state with this feature's status yet.
            FeatureMigrationResults currentResults = clusterState.metadata().getProject().custom(FeatureMigrationResults.TYPE);
            assertThat(currentResults, notNullValue());
            assertThat(currentResults.getFeatureStatuses(), allOf(aMapWithSize(1), hasKey(FEATURE_NAME)));
            assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).succeeded(), is(true));
            assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getFailedResourceName(), nullValue());
            assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getException(), nullValue());

            secondPluginPostMigrationHookCalled.set(true);
            hooksCalled.countDown();
        });

        PostFeatureUpgradeRequest migrationRequest = new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT);
        PostFeatureUpgradeResponse migrationResponse = client().execute(PostFeatureUpgradeAction.INSTANCE, migrationRequest).get();
        assertThat(migrationResponse.getReason(), nullValue());
        assertThat(migrationResponse.getElasticsearchException(), nullValue());
        final Set<String> migratingFeatures = migrationResponse.getFeatures()
            .stream()
            .map(PostFeatureUpgradeResponse.Feature::getFeatureName)
            .collect(Collectors.toSet());
        assertThat(migratingFeatures, hasItems(FEATURE_NAME, SECOND_FEATURE_NAME));

        // wait for all the plugin methods to have been called before assertBusy since that will exponentially backoff
        assertThat(hooksCalled.await(30, TimeUnit.SECONDS), is(true));

        GetFeatureUpgradeStatusRequest getStatusRequest = new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT);
        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResponse = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest)
                .get();
            logger.info(Strings.toString(statusResponse));
            assertThat(statusResponse.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });

        assertTrue("the first plugin's pre-migration hook wasn't actually called", preMigrationHookCalled.get());
        assertTrue("the first plugin's post-migration hook wasn't actually called", postMigrationHookCalled.get());

        assertTrue("the second plugin's pre-migration hook wasn't actually called", secondPluginPreMigrationHookCalled.get());
        assertTrue("the second plugin's post-migration hook wasn't actually called", secondPluginPostMigrationHookCalled.get());

        ProjectMetadata finalMetadata = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().getProject();
        // Check that the results metadata is what we expect
        FeatureMigrationResults currentResults = finalMetadata.custom(FeatureMigrationResults.TYPE);
        assertThat(currentResults, notNullValue());
        assertThat(currentResults.getFeatureStatuses(), allOf(aMapWithSize(2), hasKey(FEATURE_NAME), hasKey(SECOND_FEATURE_NAME)));
        assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).succeeded(), is(true));
        assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getFailedResourceName(), nullValue());
        assertThat(currentResults.getFeatureStatuses().get(FEATURE_NAME).getException(), nullValue());
        assertThat(currentResults.getFeatureStatuses().get(SECOND_FEATURE_NAME).succeeded(), is(true));
        assertThat(currentResults.getFeatureStatuses().get(SECOND_FEATURE_NAME).getFailedResourceName(), nullValue());
        assertThat(currentResults.getFeatureStatuses().get(SECOND_FEATURE_NAME).getException(), nullValue());

        // Finally, verify that all the indices exist and have the properties we expect.
        assertIndexHasCorrectProperties(
            finalMetadata,
            ".int-man-old" + UPGRADED_INDEX_SUFFIX,
            INTERNAL_MANAGED_FLAG_VALUE,
            true,
            true,
            Arrays.asList(".int-man-old", ".internal-managed-alias")
        );
        assertIndexHasCorrectProperties(
            finalMetadata,
            ".int-unman-old" + UPGRADED_INDEX_SUFFIX,
            INTERNAL_UNMANAGED_FLAG_VALUE,
            false,
            true,
            Collections.singletonList(".int-unman-old")
        );
        assertIndexHasCorrectProperties(
            finalMetadata,
            ".ext-man-old" + UPGRADED_INDEX_SUFFIX,
            EXTERNAL_MANAGED_FLAG_VALUE,
            true,
            false,
            Arrays.asList(".ext-man-old", ".external-managed-alias")
        );
        assertIndexHasCorrectProperties(
            finalMetadata,
            ".ext-unman-old" + UPGRADED_INDEX_SUFFIX,
            EXTERNAL_UNMANAGED_FLAG_VALUE,
            false,
            false,
            Collections.singletonList(".ext-unman-old")
        );

        assertIndexHasCorrectProperties(
            finalMetadata,
            ".second-int-man-old" + UPGRADED_INDEX_SUFFIX,
            SECOND_FEATURE_IDX_FLAG_VALUE,
            true,
            true,
            Arrays.asList(".second-int-man-old", ".second-internal-managed-alias")
        );
    }

    private static final SystemIndexDescriptor SECOND_FEATURE_IDX_DESCIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".second-int-man-*")
        .setAliasName(".second-internal-managed-alias")
        .setPrimaryIndex(".second-int-man-old")
        .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
        .setSettings(createSettings(IndexVersions.MINIMUM_COMPATIBLE, 0))
        .setMappings(createMapping(true, true))
        .setOrigin(ORIGIN)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .build();

    public static class SecondPlugin extends Plugin implements SystemIndexPlugin {

        private final AtomicReference<Function<ClusterState, Map<String, Object>>> preMigrationHook = new AtomicReference<>();
        private final AtomicReference<BiConsumer<ClusterState, Map<String, Object>>> postMigrationHook = new AtomicReference<>();

        public SecondPlugin() {}

        @Override
        public String getFeatureName() {
            return SECOND_FEATURE_NAME;
        }

        @Override
        public String getFeatureDescription() {
            return "a plugin for test system index migration with multiple features";
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(SECOND_FEATURE_IDX_DESCIPTOR);
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
