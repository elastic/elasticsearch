/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.system_indices.action.AbstractFeatureMigrationIntegTest.TestPlugin.BlockingActionFilter;
import org.elasticsearch.system_indices.task.FeatureMigrationResults;
import org.elasticsearch.system_indices.task.SingleFeatureMigrationResult;

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

import static org.elasticsearch.indices.SystemIndices.UPGRADED_INDEX_SUFFIX;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class FeatureMigrationIT extends AbstractFeatureMigrationIntegTest {
    private static final String INTERNAL_MANAGED_WITH_SCRIPT_INDEX_NAME = ".int-mans-old";
    private static final String SCRIPTED_INDEX_FEATURE_NAME = "B-test-feature";
    private static final SystemIndexDescriptor INTERNAL_MANAGED_WITH_SCRIPT = SystemIndexDescriptor.builder()
        .setIndexPattern(".int-mans-*")
        .setAliasName(".internal-managed-with-script-alias")
        .setPrimaryIndex(INTERNAL_MANAGED_WITH_SCRIPT_INDEX_NAME)
        .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
        .setSettings(createSettings(NEEDS_UPGRADE_INDEX_VERSION, INTERNAL_MANAGED_FLAG_VALUE))
        .setMappings(createMapping(true, true))
        .setOrigin(ORIGIN)
        .setAllowedElasticProductOrigins(Collections.emptyList())
        .setPriorSystemIndexDescriptors(Collections.emptyList())
        .setMigrationScript("""
            if (ctx._source.some_field != null) {
              ctx._source.some_field = 'migrated';
            }
            """)
        .build();

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
        plugins.add(SecondTestPlugin.class);
        plugins.add(PainlessPlugin.class);
        return plugins;
    }

    public void testStartMigrationAndImmediatelyCheckStatus() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_MANAGED);
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);
        createSystemIndexForDescriptor(EXTERNAL_MANAGED);
        createSystemIndexForDescriptor(EXTERNAL_UNMANAGED);

        ensureGreen();

        PostFeatureUpgradeRequest migrationRequest = new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT);
        GetFeatureUpgradeStatusRequest getStatusRequest = new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT);

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

    public void testMigrateSystemIndex() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_MANAGED);
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);
        createSystemIndexForDescriptor(EXTERNAL_MANAGED);
        createSystemIndexForDescriptor(EXTERNAL_UNMANAGED);

        CreateIndexRequestBuilder createRequest = prepareCreate(ASSOCIATED_INDEX_NAME);
        createRequest.setWaitForActiveShards(ActiveShardCount.ALL);
        createRequest.setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, NEEDS_UPGRADE_INDEX_VERSION)
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
            FeatureMigrationResults currentResults = clusterState.metadata().getProject().custom(FeatureMigrationResults.TYPE);
            assertThat(currentResults, nullValue());
            postUpgradeHookCalled.set(true);
        });

        executeMigration(FEATURE_NAME);

        // Waiting for shards to stabilize if indices were moved around
        ensureGreen();

        assertTrue("the pre-migration hook wasn't actually called", preUpgradeHookCalled.get());
        assertTrue("the post-migration hook wasn't actually called", postUpgradeHookCalled.get());

        ProjectMetadata finalMetadata = assertMetadataAfterMigration(FEATURE_NAME);

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
    }

    public void testMigrateIndexWithWriteBlock() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);

        String indexName = INTERNAL_UNMANAGED.getIndexPattern().replace("*", "old");
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), indexName);
        ensureGreen();

        client().execute(PostFeatureUpgradeAction.INSTANCE, new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT)).get();

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }

    public void testIndexBlockIsRemovedWhenAliasRequestFails() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_UNMANAGED);
        ensureGreen();

        BlockingActionFilter blockingActionFilter = blockAction(TransportIndicesAliasesAction.NAME);

        // Start the migration
        client().execute(PostFeatureUpgradeAction.INSTANCE, new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT)).get();

        // Wait till the migration fails
        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR));
        });

        // Get the settings to see if the write block was removed
        var allsettings = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, INTERNAL_UNMANAGED.getIndexPattern())
            .get()
            .getIndexToSettings();
        var internalUnmanagedOldIndexSettings = allsettings.get(".int-unman-old");
        var writeBlock = internalUnmanagedOldIndexSettings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey());
        assertThat("Write block on old index should be removed on migration ERROR status", writeBlock, equalTo("false"));

        // Unblock the alias request
        blockingActionFilter.unblockAllActions();

        // Retry the migration
        client().execute(PostFeatureUpgradeAction.INSTANCE, new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT)).get();

        // Ensure that the migration is successful after the alias request is unblocked
        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
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

        PostFeatureUpgradeRequest migrationRequest = new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT);
        PostFeatureUpgradeResponse migrationResponse = client().execute(PostFeatureUpgradeAction.INSTANCE, migrationRequest).get();
        // Make sure we actually started the migration
        assertTrue(
            "could not find [" + FEATURE_NAME + "] in response: " + Strings.toString(migrationResponse),
            migrationResponse.getFeatures().stream().anyMatch(feature -> feature.getFeatureName().equals(FEATURE_NAME))
        );

        // Now wait for the migration to finish (otherwise the test infra explodes)
        assertBusy(() -> {
            GetFeatureUpgradeStatusRequest getStatusRequest = new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT);
            GetFeatureUpgradeStatusResponse statusResp = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }

    public void testMigrateUsingScript() throws Exception {
        createSystemIndexForDescriptor(INTERNAL_MANAGED_WITH_SCRIPT);

        executeMigration(SCRIPTED_INDEX_FEATURE_NAME);
        ensureGreen();

        ProjectMetadata metadata = assertMetadataAfterMigration(SCRIPTED_INDEX_FEATURE_NAME);
        String newIndexName = ".int-mans-old" + UPGRADED_INDEX_SUFFIX;
        assertIndexHasCorrectProperties(
            metadata,
            newIndexName,
            INTERNAL_MANAGED_FLAG_VALUE,
            true,
            true,
            Arrays.asList(".int-mans-old", ".internal-managed-with-script-alias")
        );

        SearchRequestBuilder searchRequestBuilder = prepareSearch(newIndexName).setQuery(QueryBuilders.termsQuery(FIELD_NAME, "migrated"))
            .setSize(0);
        assertHitCountAndNoFailures(searchRequestBuilder, INDEX_DOC_COUNT);
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

        PostFeatureUpgradeResponse migrationResponse = client().execute(
            PostFeatureUpgradeAction.INSTANCE,
            new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT)
        ).get();

        assertTrue(migrationResponse.isAccepted());
    }

    public void testBailOnMigrateWithTemplatesV1() throws Exception {
        migrateWithTemplatesV1(".int", INTERNAL_UNMANAGED);

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
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
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
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

        ComposableIndexTemplate cit = ComposableIndexTemplate.builder()
            .indexPatterns(Collections.singletonList(prefix + "*"))
            .template(
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
                )
            )
            .componentTemplates(Collections.singletonList("a-ct"))
            .priority(4L)
            .version(5L)
            .metadata(Collections.singletonMap("baz", "thud"))
            .build();
        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request("a-it").indexTemplate(cit)
        ).get();

        ensureGreen();

        PostFeatureUpgradeResponse migrationResponse = client().execute(
            PostFeatureUpgradeAction.INSTANCE,
            new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT)
        ).get();
        assertTrue(migrationResponse.isAccepted());
    }

    public void testBailOnMigrateWithTemplatesV2() throws Exception {
        migrateWithTemplatesV2(".int", INTERNAL_UNMANAGED);

        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResp = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
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
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
            ).get();
            logger.info(Strings.toString(statusResp));
            assertThat(statusResp.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED));
        });
    }

    public static class SecondTestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public String getFeatureName() {
            return SCRIPTED_INDEX_FEATURE_NAME;
        }

        @Override
        public String getFeatureDescription() {
            return "a plugin for testing system index migration";
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(INTERNAL_MANAGED_WITH_SCRIPT);
        }
    }
}
