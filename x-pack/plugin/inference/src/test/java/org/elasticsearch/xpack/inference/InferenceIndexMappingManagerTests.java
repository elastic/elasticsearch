/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceIndexMappingManagerTests extends ESTestCase {

    private Client mockClient;
    private SystemIndexDescriptor descriptor;

    @Before
    public void setUpMocks() {
        mockClient = mock(Client.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.settings()).thenReturn(Settings.EMPTY);

        descriptor = InferencePlugin.createInferenceIndexDescriptor(InferenceIndex.settings());
    }

    /** Returns a cluster state that contains no {@code .inference} index. */
    private static ClusterState emptyClusterState() {
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();
    }

    /**
     * Returns a cluster state where the concrete index {@code .inference} exists and carries the
     * supplied mappings JSON.
     */
    private static ClusterState clusterStateWithIndex(String indexName, String mappingsJson) {
        IndexMetadata indexMeta = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(mappingsJson)))
            .build();
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMeta, false).build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> jsonToMap(String json) {
        Map<String, Object> doc = (Map<String, Object>) XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON)
            .v2()
            .get("_doc");
        assertNotNull("mapping JSON must have a top-level '_doc' key", doc);
        return doc;
    }

    public void testIndexDoesNotExist_createsIndex() {
        ClusterState clusterState = emptyClusterState();
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<CreateIndexResponse>> capturedListeners = new ArrayList<>();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("CreateIndex should have been requested exactly once", capturedListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        // The empty origin on the CreateIndexRequest is load-bearing: it routes
        // TransportCreateIndexAction to the managed-system-index branch, which builds the index
        // entirely from the descriptor compatible with the oldest node in the cluster — including
        // the hidden write alias. Setting an origin would make the master honor the request
        // verbatim instead, skipping the minimum-mappings-version guard and creating the alias
        // without the hidden/write-index flags. Any mappings/settings on the request would be
        // ignored on the managed branch, which is why the manager must not set them.
        ArgumentCaptor<CreateIndexRequest> createCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(mockClient).execute(eq(TransportCreateIndexAction.TYPE), createCaptor.capture(), any());
        assertThat("CreateIndexRequest must target the primary index", createCaptor.getValue().index(), equalTo(InferenceIndex.INDEX_NAME));
        assertThat(
            "CreateIndexRequest must not carry an origin so the master builds the index from the managed descriptor",
            createCaptor.getValue().origin(),
            is(emptyString())
        );

        // Simulate an acknowledged create response. Creation alone does not guarantee the latest
        // mappings (in a mixed-version cluster the master applies the minimum compatible descriptor),
        // so a follow-up PutMapping must be issued before the caller is notified.
        capturedListeners.get(0).onResponse(new CreateIndexResponse(true, true, InferenceIndex.INDEX_NAME));

        assertThat("PutMapping should follow the successful create", putMappingListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the put-mapping completes", callerListener.completed);

        // The follow-up PutMapping is the request that installs this node's latest mappings.
        ArgumentCaptor<PutMappingRequest> putMappingCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);
        verify(mockClient).execute(eq(TransportPutMappingAction.TYPE), putMappingCaptor.capture(), any());
        assertThat(
            "The follow-up PutMappingRequest must carry the descriptor's latest mappings",
            jsonToMap(putMappingCaptor.getValue().source()),
            equalTo(jsonToMap(descriptor.getMappings()))
        );
        assertThat(
            "The follow-up PutMappingRequest must carry the inference origin",
            putMappingCaptor.getValue().origin(),
            equalTo(ClientHelper.INFERENCE_ORIGIN)
        );

        putMappingListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after create and put-mapping complete", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testIndexAtCurrentVersion_immediateCallbackNoIO() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, descriptor.getMappings());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertTrue("Caller listener must be notified synchronously on the fast path", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
        verify(mockClient, never()).execute(any(), any(), any());
    }

    public void testIndexWithOutdatedMappings_putsMappings() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> capturedListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should have been requested exactly once", capturedListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        // Verify the PutMappingRequest was issued with the descriptor's latest mappings.
        ArgumentCaptor<PutMappingRequest> putMappingCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);
        verify(mockClient).execute(eq(TransportPutMappingAction.TYPE), putMappingCaptor.capture(), any());
        assertThat(
            "PutMappingRequest must carry the descriptor's latest mappings",
            jsonToMap(putMappingCaptor.getValue().source()),
            equalTo(jsonToMap(descriptor.getMappings()))
        );
        assertThat(
            "PutMappingRequest must carry the inference origin so an older master does not reject "
                + "mappings that differ from its own descriptor",
            putMappingCaptor.getValue().origin(),
            equalTo(ClientHelper.INFERENCE_ORIGIN)
        );

        // Simulate an acknowledged put-mapping response.
        capturedListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
        // Verify no CreateIndex was issued.
        verify(mockClient, never()).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
    }

    public void testResourceAlreadyExistsOnCreate_fallsThroughToPutMapping() {
        ClusterState clusterState = emptyClusterState();
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            listener.onFailure(new ResourceAlreadyExistsException(InferenceIndex.INDEX_NAME));
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should be issued after ResourceAlreadyExistsException", putMappingListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        putMappingListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testPutMappingFailure_propagatesToListener() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        RuntimeException mappingError = new RuntimeException("simulated put-mapping failure");
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onFailure(mappingError);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertTrue("Caller listener must receive the failure", callerListener.completed);
        assertSame("The original exception must be forwarded unchanged", mappingError, callerListener.failure);
    }

    public void testPutMappingNotAcknowledged_failsWithRetryableStatus() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(AcknowledgedResponse.of(false));
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertTrue("Caller listener must receive the failure", callerListener.completed);
        assertThat(callerListener.failure, instanceOf(ElasticsearchStatusException.class));
        assertThat(
            "An unacknowledged put-mapping usually means a busy master and must be reported as retryable",
            ((ElasticsearchStatusException) callerListener.failure).status(),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );
    }

    public void testPutMappingIndexNotFound_fallsBackToCreateIndex() {
        // The local cluster state says the index exists with stale mappings, but it is deleted
        // (e.g. DELETE .inference or a feature-state reset) before the put-mapping is processed.
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());
        List<ActionListener<CreateIndexResponse>> createListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            createListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should have been requested first", putMappingListeners, hasSize(1));
        putMappingListeners.get(0).onFailure(new IndexNotFoundException(InferenceIndex.INDEX_NAME));

        assertThat("CreateIndex must be issued when the index vanished under the put-mapping", createListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the fallback completes", callerListener.completed);

        createListeners.get(0).onResponse(new CreateIndexResponse(true, true, InferenceIndex.INDEX_NAME));

        assertThat("A follow-up PutMapping should follow the successful create", putMappingListeners, hasSize(2));
        putMappingListeners.get(1).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after the create fallback completes", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testPutMappingIndexNotFoundAfterCreateFallback_failurePropagates() {
        // Pathological ping-pong scenario: put-mapping fails with IndexNotFoundException, the create
        // fallback reports the index already exists, and the second put-mapping fails with
        // IndexNotFoundException again. The cycle must not loop: the failure is propagated.
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());
        List<ActionListener<CreateIndexResponse>> createListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            createListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should have been requested first", putMappingListeners, hasSize(1));
        putMappingListeners.get(0).onFailure(new IndexNotFoundException(InferenceIndex.INDEX_NAME));

        assertThat("CreateIndex must be issued as the fallback", createListeners, hasSize(1));
        createListeners.get(0).onFailure(new ResourceAlreadyExistsException(InferenceIndex.INDEX_NAME));

        assertThat("The create fallback falls through to a second PutMapping", putMappingListeners, hasSize(2));
        IndexNotFoundException secondFailure = new IndexNotFoundException(InferenceIndex.INDEX_NAME);
        putMappingListeners.get(1).onFailure(secondFailure);

        assertThat("No second CreateIndex may be issued — the fallback must not loop", createListeners, hasSize(1));
        assertTrue("Caller listener must receive the failure", callerListener.completed);
        assertSame("The put-mapping failure must be forwarded unchanged", secondFailure, callerListener.failure);
    }

    public void testCreateIndexRejectedAsExistingAlias_fallsThroughToPutMapping() {
        // The local cluster state says the index is absent, but by the time the create reaches the
        // master the name exists as an alias (post system-index-migration). For an existing alias the
        // master rejects the create with InvalidIndexNameException, not ResourceAlreadyExistsException.
        ClusterState clusterState = emptyClusterState();
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            listener.onFailure(new InvalidIndexNameException(InferenceIndex.INDEX_NAME, "already exists as alias"));
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should be issued after InvalidIndexNameException", putMappingListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        putMappingListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testCreateIndexNotAcknowledged_fallsThroughToPutMapping() {
        // An unacknowledged create means the master applied the cluster state update but not all nodes
        // acked it within the timeout — the index exists, so the manager must proceed to put-mapping
        // instead of failing what is a recoverable slow-cluster condition.
        ClusterState clusterState = emptyClusterState();
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            listener.onResponse(new CreateIndexResponse(false, false, InferenceIndex.INDEX_NAME));
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should be issued despite the unacknowledged create", putMappingListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        putMappingListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testAliasResolution_currentMappings_immediateCallback() {
        // Simulates: ".inference" is an alias pointing to ".inference-reindexed-for-10"
        // (the pattern produced by SystemIndexMigrator).
        String migratedIndexName = ".inference-reindexed-for-10";
        IndexMetadata migratedIndex = IndexMetadata.builder(migratedIndexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(descriptor.getMappings())))
            .putAlias(AliasMetadata.builder(InferenceIndex.INDEX_NAME).build())
            .build();
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(migratedIndex, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();

        // Confirm the alias setup: projectMetadata.index(".inference") must return null so
        // InferenceIndexMappingManager falls through to the indicesLookup resolution path.
        assertNull("'.inference' should not resolve as a concrete index name", project.index(InferenceIndex.INDEX_NAME));

        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertTrue("Caller listener must be notified synchronously — mappings are already current", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
        verify(mockClient, never()).execute(any(), any(), any());
    }

    public void testAliasResolution_staleMappings_putsMappings() {
        // Simulates: ".inference" is an alias pointing to ".inference-reindexed-for-10"
        // (the pattern produced by SystemIndexMigrator), but the concrete index still
        // has v3 (stale) mappings — e.g. the migration happened before the mapping upgrade.
        String migratedIndexName = ".inference-reindexed-for-10";
        IndexMetadata migratedIndex = IndexMetadata.builder(migratedIndexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(InferenceIndex.mappingsV3())))
            .putAlias(AliasMetadata.builder(InferenceIndex.INDEX_NAME).build())
            .build();
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(migratedIndex, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();

        // Confirm the alias setup: projectMetadata.index(".inference") must return null so
        // InferenceIndexMappingManager falls through to the indicesLookup resolution path.
        assertNull("'.inference' should not resolve as a concrete index name", project.index(InferenceIndex.INDEX_NAME));

        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> capturedListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should have been requested exactly once", capturedListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        // Verify no CreateIndex was issued — the index exists (behind the alias), only mappings need updating.
        verify(mockClient, never()).execute(eq(TransportCreateIndexAction.TYPE), any(), any());

        // Simulate an acknowledged put-mapping response.
        capturedListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    /**
     * Pins the version-parsing contract the manager relies on via
     * {@code SystemIndexMappingUpdateService.checkIndexMappingUpToDate}: genuinely older mappings
     * versions (v1–v3), as well as mappings whose version cannot be read (no {@code _meta}, no
     * {@code managed_index_mappings_version} key, or a non-integer value — all treated as version
     * {@code -1}), must all be considered outdated and trigger a mapping update.
     */
    public void testOutdatedOrUnreadableMappingsVersion_triggersPutMapping() {
        Map<String, String> outdatedMappingsByCase = Map.of(
            "v1 mappings",
            InferenceIndex.mappingsV1(),
            "v2 mappings",
            InferenceIndex.mappingsV2(),
            "v3 mappings",
            InferenceIndex.mappingsV3(),
            "mapping without _meta",
            """
                {"_doc":{"dynamic":"strict","properties":{"model_id":{"type":"keyword"}}}}""",
            "_meta without a version key",
            """
                {"_doc":{"_meta":{"some_other_key":1},"dynamic":"strict","properties":{"model_id":{"type":"keyword"}}}}""",
            "non-integer version value",
            """
                {"_doc":{"_meta":{"managed_index_mappings_version":"not-a-number"},"dynamic":"strict",\
                "properties":{"model_id":{"type":"keyword"}}}}"""
        );

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        for (var entry : outdatedMappingsByCase.entrySet()) {
            putMappingListeners.clear();
            // A fresh manager per case so no in-flight or memoized state leaks between cases.
            InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);
            ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, entry.getValue());

            TestActionListener callerListener = new TestActionListener();
            manager.withUpToDateMappings(clusterState, callerListener);

            assertThat("A PutMapping must be issued for: " + entry.getKey(), putMappingListeners, hasSize(1));
            assertFalse("Caller listener should not be notified until the I/O completes: " + entry.getKey(), callerListener.completed);

            putMappingListeners.get(0).onResponse(AcknowledgedResponse.of(true));

            assertTrue("Caller listener must be notified after successful put-mapping: " + entry.getKey(), callerListener.completed);
            assertNull("No failure should be reported: " + entry.getKey(), callerListener.failure);
        }
        verify(mockClient, never()).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
    }

    public void testUpToDateMemo_doesNotShortCircuitWhenMappingsChange() {
        // Guards the up-to-date memoization: after a successful check is memoized, a state whose
        // mapping content differs (e.g. an older mapping reappearing after a migration or restore)
        // must not be wrongly short-circuited — it must trigger a mapping update.
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        // Two calls with up-to-date mappings: the first parses and memoizes, the second hits the memo.
        ClusterState upToDateState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, descriptor.getMappings());
        for (int i = 0; i < 2; i++) {
            TestActionListener callerListener = new TestActionListener();
            manager.withUpToDateMappings(upToDateState, callerListener);
            assertTrue("Caller listener must be notified synchronously on the fast path", callerListener.completed);
            assertNull("No failure should be reported", callerListener.failure);
        }
        verify(mockClient, never()).execute(any(), any(), any());

        // A state with different (stale) mapping content must bypass the memo and issue a PutMapping.
        ClusterState staleState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(staleState, callerListener);

        assertThat("The stale mappings must trigger a PutMapping despite the memoized check", putMappingListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);
    }

    public void testConcurrentCalls_singleUpdateNotifiesAllCallers() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> capturedListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener caller1 = new TestActionListener();
        TestActionListener caller2 = new TestActionListener();

        // First call acquires the in-flight guard and issues the PutMapping request.
        manager.withUpToDateMappings(clusterState, caller1);
        // Second call arrives while the update is in-flight; it must be queued, not issue a new request.
        manager.withUpToDateMappings(clusterState, caller2);

        assertThat("Exactly one PutMapping must be issued for both concurrent callers", capturedListeners, hasSize(1));
        assertFalse("Caller 1 must not be notified yet", caller1.completed);
        assertFalse("Caller 2 must not be notified yet", caller2.completed);

        // Completing the single in-flight update must notify both callers.
        capturedListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller 1 must be notified after the update completes", caller1.completed);
        assertNull("Caller 1 must not see a failure", caller1.failure);
        assertTrue("Caller 2 must be notified after the update completes", caller2.completed);
        assertNull("Caller 2 must not see a failure", caller2.failure);
    }

    /** Simple {@link ActionListener} that records whether it was called and with what outcome. */
    private static class TestActionListener implements ActionListener<Void> {
        boolean completed = false;
        Exception failure = null;

        @Override
        public void onResponse(Void v) {
            completed = true;
        }

        @Override
        public void onFailure(Exception e) {
            completed = true;
            failure = e;
        }
    }
}
