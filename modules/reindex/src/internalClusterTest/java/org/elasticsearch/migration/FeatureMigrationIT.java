/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.migration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusAction;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusRequest;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeAction;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeRequest;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.upgrades.FeatureMigrationResults;
import org.elasticsearch.upgrades.SingleFeatureMigrationResult;

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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FeatureMigrationIT extends AbstractFeatureMigrationIntegTest {
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
        getPlugin(TestPlugin.class).preMigrationHook.set(clusterState -> {
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

        getPlugin(TestPlugin.class).postMigrationHook.set((clusterState, metadata) -> {
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
        // The feature upgrade may take longer than ten seconds when tests are running
        // in parallel, so we give assertBusy a sixty-second timeout.
        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResponse = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest)
                .get();
            logger.info(Strings.toString(statusResponse));
            assertThat(statusResponse.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        }, 60, TimeUnit.SECONDS);

        // Waiting for shards to stabilize if indices were moved around
        ensureGreen();

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

        String indexName = INTERNAL_UNMANAGED.getIndexPattern().replace("*", "old");
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), indexName);
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

    public void testMigrationWillRunAfterError() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_MANAGED);

        ensureGreen();

        SetOnce<Exception> failure = new SetOnce<>();
        CountDownLatch clusterStateUpdated = new CountDownLatch(1);
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask(this.getTestName(), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    FeatureMigrationResults newResults = new FeatureMigrationResults(
                        Collections.singletonMap(
                            FEATURE_NAME,
                            SingleFeatureMigrationResult.failure(INTERNAL_MANAGED_INDEX_NAME, new RuntimeException("it failed :("))
                        )
                    );
                    Metadata newMetadata = Metadata.builder(currentState.metadata())
                        .putCustom(FeatureMigrationResults.TYPE, newResults)
                        .build();
                    return ClusterState.builder(currentState).metadata(newMetadata).build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    clusterStateUpdated.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.set(e);
                    clusterStateUpdated.countDown();
                }
            });

        clusterStateUpdated.await(10, TimeUnit.SECONDS); // Should be basically instantaneous
        if (failure.get() != null) {
            logger.error("cluster state update to inject migration failure state did not succeed", failure.get());
            fail("cluster state update failed, see log for details");
        }

        PostFeatureUpgradeRequest migrationRequest = new PostFeatureUpgradeRequest();
        PostFeatureUpgradeResponse migrationResponse = client().execute(PostFeatureUpgradeAction.INSTANCE, migrationRequest).get();
        // Make sure we actually started the migration
        assertTrue(
            "could not find [" + FEATURE_NAME + "] in response: " + Strings.toString(migrationResponse),
            migrationResponse.getFeatures().stream().anyMatch(feature -> feature.getFeatureName().equals(FEATURE_NAME))
        );

        // Now wait for the migration to finish (otherwise the test infra explodes)
        assertBusy(() -> {
            GetFeatureUpgradeStatusRequest getStatusRequest = new GetFeatureUpgradeStatusRequest();
            GetFeatureUpgradeStatusResponse statusResp = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }

    private String featureUpgradeErrorResponse(GetFeatureUpgradeStatusResponse statusResp) {
        return statusResp.getFeatureUpgradeStatuses()
            .stream()
            .map(f -> f.getIndexVersions())
            .flatMap(List::stream)
            .map(i -> (i.getException() == null) ? "" : i.getException().getMessage())
            .collect(Collectors.joining(" "));
    }

    private void migrateWithTemplatesV1(String templatePrefix, SystemIndexDescriptor... descriptors) throws Exception {
        for (SystemIndexDescriptor descriptor : descriptors) {
            createSystemIndexForDescriptor(descriptor);
        }

        indicesAdmin().preparePutTemplate("bad_template")
            .setPatterns(Collections.singletonList(templatePrefix + "*"))
            .addAlias(new Alias(templatePrefix + "-legacy-alias"))
            .get();

        ensureGreen();

        PostFeatureUpgradeResponse migrationResponse = client().execute(PostFeatureUpgradeAction.INSTANCE, new PostFeatureUpgradeRequest())
            .get();

        assertTrue(migrationResponse.isAccepted());
    }

    public void testBailOnMigrateWithTemplatesV1() throws Exception {
        migrateWithTemplatesV1(".int", INTERNAL_UNMANAGED);

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest()
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR));
            assertTrue(featureUpgradeErrorResponse(statusResp).contains(" because it would match legacy templates "));
        });
    }

    public void testMigrateWithTemplatesV1() throws Exception {
        // this should pass for both, kibana allows templates, the unmanaged doesn't match the template
        migrateWithTemplatesV1(".kibana", KIBANA_MOCK_INDEX_DESCRIPTOR, INTERNAL_UNMANAGED);

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest()
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }

    private void migrateWithTemplatesV2(String prefix, SystemIndexDescriptor... descriptors) throws Exception {
        for (SystemIndexDescriptor descriptor : descriptors) {
            createSystemIndexForDescriptor(descriptor);
        }

        ComponentTemplate ct = new ComponentTemplate(
            new Template(
                null,
                new CompressedXContent(
                    "{\n"
                        + "      \"dynamic\": false,\n"
                        + "      \"properties\": {\n"
                        + "        \"field1\": {\n"
                        + "          \"type\": \"text\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }"
                ),
                null
            ),
            3L,
            Collections.singletonMap("foo", "bar")
        );
        client().execute(PutComponentTemplateAction.INSTANCE, new PutComponentTemplateAction.Request("a-ct").componentTemplate(ct)).get();

        ComposableIndexTemplate cit = new ComposableIndexTemplate(
            Collections.singletonList(prefix + "*"),
            new Template(
                null,
                new CompressedXContent(
                    "{\n"
                        + "      \"dynamic\": false,\n"
                        + "      \"properties\": {\n"
                        + "        \"field2\": {\n"
                        + "          \"type\": \"keyword\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }"
                ),
                null
            ),
            Collections.singletonList("a-ct"),
            4L,
            5L,
            Collections.singletonMap("baz", "thud")
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, new PutComposableIndexTemplateAction.Request("a-it").indexTemplate(cit))
            .get();

        ensureGreen();

        PostFeatureUpgradeResponse migrationResponse = client().execute(PostFeatureUpgradeAction.INSTANCE, new PostFeatureUpgradeRequest())
            .get();
        assertTrue(migrationResponse.isAccepted());
    }

    public void testBailOnMigrateWithTemplatesV2() throws Exception {
        migrateWithTemplatesV2(".int", INTERNAL_UNMANAGED);

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest()
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR));
            assertTrue(featureUpgradeErrorResponse(statusResp).contains(" it would match composable template [a-it]"));
        });
    }

    public void testMigrateWithTemplatesV2() throws Exception {
        // this should pass for both, kibana allows templates, the unmanaged doesn't match the template
        migrateWithTemplatesV2(".kibana", KIBANA_MOCK_INDEX_DESCRIPTOR, INTERNAL_UNMANAGED);

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest()
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }
}
