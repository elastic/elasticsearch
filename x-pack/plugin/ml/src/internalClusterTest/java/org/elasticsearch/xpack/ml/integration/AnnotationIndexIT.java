/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.SetResetModeActionRequest;
import org.elasticsearch.xpack.core.ml.action.SetResetModeAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.hamcrest.Matchers.is;

public class AnnotationIndexIT extends MlSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        return newSettings.build();
    }

    @Before
    public void createComponents() throws Exception {
        waitForMlTemplates();
    }

    public void testNotCreatedWhenNoOtherMlIndices() {

        // Ask a few times to increase the chance of failure if the .ml-annotations index is created when no other ML index exists
        for (int i = 0; i < 10; ++i) {
            assertFalse(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
            assertEquals(0, numberOfAnnotationsAliases());
        }
    }

    public void testCreatedWhenAfterOtherMlIndex() throws Exception {
        // Creating a document in the .ml-notifications-000002 index should cause .ml-annotations
        // to be created, as it should get created as soon as any other ML index exists
        createNotification();

        assertBusy(() -> {
            assertTrue(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
            assertEquals(2, numberOfAnnotationsAliases());
        });
    }

    public void testReindexing() throws Exception {
        // Creating a document in the .ml-notifications-000002 index should cause .ml-annotations
        // to be created, as it should get created as soon as any other ML index exists
        createNotification();

        assertBusy(() -> {
            assertTrue(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
            assertEquals(2, numberOfAnnotationsAliases());
        });

        client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(true)).actionGet();

        String reindexedIndexName = ".reindexed-v7-ml-annotations-6";
        createReindexedIndex(reindexedIndexName);

        IndicesAliasesRequestBuilder indicesAliasesRequestBuilder = client().admin()
            .indices()
            .prepareAliases()
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add().index(reindexedIndexName).alias(AnnotationIndex.READ_ALIAS_NAME).isHidden(true)
            )
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add().index(reindexedIndexName).alias(AnnotationIndex.WRITE_ALIAS_NAME).isHidden(true)
            )
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(AnnotationIndex.INDEX_NAME))
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add().index(reindexedIndexName).alias(AnnotationIndex.INDEX_NAME).isHidden(true)
            );

        client().admin().indices().aliases(indicesAliasesRequestBuilder.request()).actionGet();

        client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(false)).actionGet();

        // Ask a few times to increase the chance of failure if the .ml-annotations index is created when no other ML index exists
        for (int i = 0; i < 10; ++i) {
            assertFalse(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
            assertTrue(annotationsIndexExists(reindexedIndexName));
            // Aliases should be read, write and original name
            assertEquals(3, numberOfAnnotationsAliases());
        }
    }

    public void testReindexingWithLostAliases() throws Exception {
        // Creating a document in the .ml-notifications-000002 index should cause .ml-annotations
        // to be created, as it should get created as soon as any other ML index exists
        createNotification();

        assertBusy(() -> {
            assertTrue(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
            assertEquals(2, numberOfAnnotationsAliases());
        });

        client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(true)).actionGet();

        String reindexedIndexName = ".reindexed-v7-ml-annotations-6";
        createReindexedIndex(reindexedIndexName);

        IndicesAliasesRequestBuilder indicesAliasesRequestBuilder = client().admin()
            .indices()
            .prepareAliases()
            // The difference compared to the standard reindexing test is that the read and write aliases are not correctly set up.
            // The annotations index maintenance code should add them back.
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(AnnotationIndex.INDEX_NAME))
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add().index(reindexedIndexName).alias(AnnotationIndex.INDEX_NAME).isHidden(true)
            );

        client().admin().indices().aliases(indicesAliasesRequestBuilder.request()).actionGet();

        client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(false)).actionGet();

        assertBusy(() -> {
            assertFalse(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
            assertTrue(annotationsIndexExists(reindexedIndexName));
            // Aliases should be read, write and original name
            assertEquals(3, numberOfAnnotationsAliases());
        });
    }

    public void testNotCreatedWhenAfterOtherMlIndexAndUpgradeInProgress() throws Exception {

        client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(true)).actionGet();

        try {
            // Creating a document in the .ml-notifications-000002 index would normally cause .ml-annotations
            // to be created, but in this case it shouldn't as we're doing an upgrade
            createNotification();

            assertBusy(() -> {
                try {
                    SearchResponse response = client().search(new SearchRequest(".ml-notifications*")).actionGet();
                    assertEquals(1, response.getHits().getHits().length);
                } catch (SearchPhaseExecutionException e) {
                    throw new AssertionError("Notifications index exists but shards not yet ready - continuing busy wait", e);
                }
                assertFalse(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
                assertEquals(0, numberOfAnnotationsAliases());
            });
        } finally {
            client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(false)).actionGet();
        }
    }

    public void testNotCreatedWhenAfterOtherMlIndexAndResetInProgress() throws Exception {

        client().execute(SetResetModeAction.INSTANCE, SetResetModeActionRequest.enabled()).actionGet();

        try {

            IndexRequest stateDoc = new IndexRequest(".ml-state");
            stateDoc.source(Collections.singletonMap("state", "blah"));
            IndexResponse indexResponse = client().index(stateDoc).actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status());

            // Creating the .ml-state index would normally cause .ml-annotations
            // to be created, but in this case it shouldn't as we're doing a reset

            assertBusy(() -> {
                SearchResponse response = client().search(new SearchRequest(".ml-state")).actionGet();
                assertEquals(1, response.getHits().getHits().length);
                assertFalse(annotationsIndexExists(AnnotationIndex.INDEX_NAME));
                assertEquals(0, numberOfAnnotationsAliases());
            });
        } finally {
            client().execute(SetResetModeAction.INSTANCE, SetResetModeActionRequest.disabled(true)).actionGet();
        }
    }

    private boolean annotationsIndexExists(String expectedName) {
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .prepareGetIndex()
            .setIndices(AnnotationIndex.INDEX_NAME)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .execute()
            .actionGet();
        return Arrays.asList(getIndexResponse.getIndices()).contains(expectedName);
    }

    private int numberOfAnnotationsAliases() {
        int count = 0;
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = client().admin()
            .indices()
            .prepareGetAliases(AnnotationIndex.READ_ALIAS_NAME, AnnotationIndex.WRITE_ALIAS_NAME, AnnotationIndex.INDEX_NAME)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .get()
            .getAliases();
        if (aliases != null) {
            for (ObjectObjectCursor<String, List<AliasMetadata>> entry : aliases) {
                for (AliasMetadata aliasMetadata : entry.value) {
                    assertThat("Annotations aliases should be hidden but are not: " + aliases, aliasMetadata.isHidden(), is(true));
                }
                count += entry.value.size();
            }
        }
        return count;
    }

    private void createReindexedIndex(String reindexedIndexName) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(reindexedIndexName).mapping(
            SINGLE_MAPPING_NAME,
            AnnotationIndex.annotationsMapping(),
            XContentType.JSON
        )
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                    .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            );

        client().admin().indices().create(createIndexRequest).actionGet();

        // At this point the upgrade assistant would reindex the old index into the new index but there's
        // no point in this test as there's nothing in the old index.
    }

    private void createNotification() {
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(client(), getInstanceFromNode(ClusterService.class));
        auditor.info("whatever", "blah");
    }
}
