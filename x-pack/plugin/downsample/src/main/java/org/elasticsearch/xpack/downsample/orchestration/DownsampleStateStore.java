/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ROLLUP_ORIGIN;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.DOC_TYPE_FIELD;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.INDEX_NAME;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.INDEX_TASK_DOC_TYPE;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.SHARD_TASK_DOC_TYPE;

/**
 * The only way to read and write {@link DownsampleStateIndex}. Access is split by role so that a caller can only reach the
 * operations its role allows: {@link #forMaster()} for the orchestration on the elected master, {@link #forLocalNode()} for
 * the node that claims and runs shard tasks. Every write is validated with {@link StateTransitions} and sent as a compare-and-set
 * (a create, or an index with {@code if_seq_no} / {@code if_primary_term}), so a concurrent change makes the write fail with a
 * {@link VersionConflictEngineException} instead of being overwritten; the caller then re-reads and re-decides.
 * <p>
 * Each project has its own state index. Every request is sent to the state index of the project it is about, whatever the
 * project of the caller (see {@link ProjectContext}), and every document read is checked to belong to that project.
 * <p>
 * All requests use the {@link org.elasticsearch.xpack.core.ClientHelper#ROLLUP_ORIGIN} origin, which is also the origin of
 * the system index descriptor. Searches are near real time; gets are real time.
 * <p>
 * The enforcement is in code: it protects against bugs and stale actors (an old master, a node that lost its task), not
 * against a compromised node.
 */
public class DownsampleStateStore {

    private static final Logger logger = LogManager.getLogger(DownsampleStateStore.class);

    /**
     * Searches are not paginated in the proof of concept, so they fail instead of silently returning a partial result.
     */
    static final int MAX_SEARCH_HITS = 10_000;

    private final Client client;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;

    public DownsampleStateStore(Client client, ClusterService clusterService, ProjectResolver projectResolver) {
        this.client = new OriginSettingClient(client, ROLLUP_ORIGIN);
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
    }

    /**
     * A document with the sequence number and primary term it was read or written at, needed for the next compare-and-set.
     */
    public record Versioned<T extends StateDoc>(T doc, long seqNo, long primaryTerm) {}

    /**
     * The projects that have a state index, i.e. that ever had a downsample operation through the system index. The others have
     * nothing to orchestrate or run.
     */
    public static List<ProjectId> projectsWithStateIndex(ClusterState state) {
        List<ProjectId> projects = new ArrayList<>();
        for (ProjectMetadata project : state.metadata().projects().values()) {
            if (project.index(INDEX_NAME) != null) {
                projects.add(project.id());
            }
        }
        return projects;
    }

    public MasterView forMaster() {
        return new MasterView();
    }

    public NodeView forLocalNode() {
        return new NodeView(clusterService.localNode().getId());
    }

    /**
     * The operations of the elected master. Every write first checks that the local node is still the elected master; the
     * compare-and-set covers the remaining race with a newer master.
     */
    public final class MasterView {

        private MasterView() {}

        /**
         * Creates the index task document of a new operation. If a document for the same target index already exists and is
         * not terminal it is returned unchanged, which makes the call idempotent; a terminal one is replaced.
         */
        public void createIndexTask(IndexTaskDoc doc, ActionListener<Versioned<IndexTaskDoc>> listener) {
            createIndexTask(doc, true, listener);
        }

        private void createIndexTask(IndexTaskDoc doc, boolean retryOnConflict, ActionListener<Versioned<IndexTaskDoc>> listener) {
            ActionListener.run(listener, l -> {
                ensureMaster();
                get(doc.projectId(), doc.id(), IndexTaskDoc::fromXContent, l.delegateFailureAndWrap((delegate, existing) -> {
                    if (existing.isPresent() && existing.get().doc().phase().isTerminal() == false) {
                        delegate.onResponse(existing.get());
                        return;
                    }
                    Versioned<IndexTaskDoc> current = existing.orElse(null);
                    StateTransitions.validateMasterIndexTaskWrite(current == null ? null : current.doc(), doc);
                    write(doc, current, ActionListener.wrap(delegate::onResponse, e -> {
                        if (retryOnConflict && ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                            // a concurrent request for the same target won: join its operation
                            createIndexTask(doc, false, delegate);
                        } else {
                            delegate.onFailure(e);
                        }
                    }));
                }));
            });
        }

        public void getIndexTask(ProjectId projectId, String targetIndex, ActionListener<Optional<Versioned<IndexTaskDoc>>> listener) {
            get(projectId, DownsampleStateIndex.indexTaskId(targetIndex), IndexTaskDoc::fromXContent, listener);
        }

        public void updateIndexTask(Versioned<IndexTaskDoc> current, IndexTaskDoc next, ActionListener<Versioned<IndexTaskDoc>> listener) {
            ActionListener.run(listener, l -> {
                ensureMaster();
                ensureSameDoc(current.doc(), next);
                StateTransitions.validateMasterIndexTaskWrite(current.doc(), next);
                write(next, current, l);
            });
        }

        /**
         * The operations of a project that are not finished.
         */
        public void findActiveIndexTasks(ProjectId projectId, ActionListener<List<Versioned<IndexTaskDoc>>> listener) {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, INDEX_TASK_DOC_TYPE))
                .mustNot(
                    QueryBuilders.termsQuery(
                        IndexTaskDoc.PHASE.getPreferredName(),
                        IndexTaskPhase.DONE.name(),
                        IndexTaskPhase.FAILED.name()
                    )
                );
            search(projectId, query, IndexTaskDoc::fromXContent, listener);
        }

        /**
         * Creates a shard task document. If it already exists (e.g. a new master re-running the start of the shard phase), the
         * existing document is returned unchanged.
         */
        public void createShardTask(ShardTaskDoc doc, ActionListener<Versioned<ShardTaskDoc>> listener) {
            ActionListener.run(listener, l -> {
                ensureMaster();
                StateTransitions.validateMasterShardTaskWrite(null, doc);
                write(doc, null, ActionListener.wrap(l::onResponse, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        get(doc.projectId(), doc.id(), ShardTaskDoc::fromXContent, l.map(existing -> existing.orElseThrow(() -> e)));
                    } else {
                        l.onFailure(e);
                    }
                }));
            });
        }

        /**
         * All the shard tasks of one operation. Near real time, prefer {@link #getShardTasks}.
         */
        public void findShardTasks(
            ProjectId projectId,
            String targetIndex,
            String targetIndexUuid,
            ActionListener<List<Versioned<ShardTaskDoc>>> listener
        ) {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, SHARD_TASK_DOC_TYPE))
                .filter(QueryBuilders.termQuery(IndexTaskDoc.TARGET_INDEX.getPreferredName(), targetIndex))
                .filter(QueryBuilders.termQuery(IndexTaskDoc.TARGET_INDEX_UUID.getPreferredName(), targetIndexUuid));
            search(projectId, query, ShardTaskDoc::fromXContent, listener);
        }

        /**
         * Real time read of the shard tasks of one operation, one entry per shard (empty if the document does not exist). The
         * ids are deterministic, so unlike {@link #findShardTasks} this never misses a recent write.
         */
        public void getShardTasks(
            ProjectId projectId,
            String targetIndex,
            String targetIndexUuid,
            int numberOfShards,
            ActionListener<List<Optional<Versioned<ShardTaskDoc>>>> listener
        ) {
            MultiGetRequest request = new MultiGetRequest().realtime(true);
            for (int shard = 0; shard < numberOfShards; shard++) {
                request.add(INDEX_NAME, DownsampleStateIndex.shardTaskId(targetIndex, targetIndexUuid, shard));
            }
            inProject(projectId, listener.<MultiGetResponse>delegateFailureAndWrap((l, response) -> {
                List<Optional<Versioned<ShardTaskDoc>>> docs = new ArrayList<>(numberOfShards);
                for (MultiGetItemResponse item : response.getResponses()) {
                    if (item.isFailed()) {
                        if (ExceptionsHelper.unwrapCause(item.getFailure().getFailure()) instanceof IndexNotFoundException) {
                            docs.add(Optional.empty());
                            continue;
                        }
                        throw item.getFailure().getFailure();
                    }
                    GetResponse get = item.getResponse();
                    docs.add(
                        get.isExists()
                            ? Optional.of(
                                versioned(
                                    projectId,
                                    get.getSourceAsBytesRef(),
                                    ShardTaskDoc::fromXContent,
                                    get.getSeqNo(),
                                    get.getPrimaryTerm()
                                )
                            )
                            : Optional.empty()
                    );
                }
                l.onResponse(docs);
            }), l -> client.multiGet(request, l));
        }

        public void updateShardTask(Versioned<ShardTaskDoc> current, ShardTaskDoc next, ActionListener<Versioned<ShardTaskDoc>> listener) {
            ActionListener.run(listener, l -> {
                ensureMaster();
                ensureSameDoc(current.doc(), next);
                StateTransitions.validateMasterShardTaskWrite(current.doc(), next);
                write(next, current, l);
            });
        }

        private void ensureMaster() {
            if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
                throw new NotMasterException("only the elected master can write the downsample orchestration state");
            }
        }
    }

    /**
     * The operations of a node that runs shard tasks. The node id is always the local node's, so a node cannot act as another.
     */
    public final class NodeView {

        private final String nodeId;

        private NodeView(String nodeId) {
            this.nodeId = Objects.requireNonNull(nodeId);
        }

        public String nodeId() {
            return nodeId;
        }

        /**
         * The shard tasks of a project this node currently owns. Near real time: a recent claim can be missing, a recent release
         * can still show up, so confirm with {@link #getShardTask} before acting on a removal.
         */
        public void findMyShardTasks(ProjectId projectId, ActionListener<List<Versioned<ShardTaskDoc>>> listener) {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, SHARD_TASK_DOC_TYPE))
                .filter(QueryBuilders.termQuery(ShardTaskDoc.ASSIGNED_NODE.getPreferredName(), nodeId))
                .filter(QueryBuilders.termQuery(ShardTaskDoc.STATUS.getPreferredName(), ShardTaskStatus.RUNNING.name()));
            search(projectId, query, ShardTaskDoc::fromXContent, listener);
        }

        /**
         * Up to {@code size} unassigned shard tasks of a project on the given source indices, oldest operation first, so that
         * the operations requested first finish first. Near real time: a task can show up after it was claimed, the claim then
         * fails on the compare-and-set.
         *
         * @param sourceIndexUuids the source indices this node holds shards of; tasks of other indices cannot be claimed here
         */
        public void findClaimableShardTasks(
            ProjectId projectId,
            Collection<String> sourceIndexUuids,
            int size,
            ActionListener<List<Versioned<ShardTaskDoc>>> listener
        ) {
            if (sourceIndexUuids.isEmpty() || size <= 0) {
                listener.onResponse(List.of());
                return;
            }
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, SHARD_TASK_DOC_TYPE))
                .filter(QueryBuilders.termQuery(ShardTaskDoc.STATUS.getPreferredName(), ShardTaskStatus.UNASSIGNED.name()))
                .filter(QueryBuilders.termsQuery(IndexTaskDoc.SOURCE_INDEX_UUID.getPreferredName(), sourceIndexUuids));
            search(
                projectId,
                new SearchSourceBuilder().query(query)
                    .size(size)
                    .sort(IndexTaskDoc.CREATED_AT.getPreferredName(), SortOrder.ASC)
                    .sort(ShardTaskDoc.SHARD.getPreferredName(), SortOrder.ASC),
                false,
                ShardTaskDoc::fromXContent,
                listener
            );
        }

        /**
         * Claims an unassigned shard task for this node and marks it running, in one compare-and-set. Only allowed while this
         * node holds a started, searchable copy of the source shard, since the shard is read locally, and is not marked for
         * shutdown, so that a node about to leave does not pick up new work.
         *
         * @param runId    identifies the local run that will execute the task
         * @param progress the initial counters of that run
         */
        public void claimShardTask(
            Versioned<ShardTaskDoc> current,
            String runId,
            Map<String, Object> progress,
            ActionListener<Versioned<ShardTaskDoc>> listener
        ) {
            ActionListener.run(listener, l -> {
                ShardTaskDoc next = current.doc().claimBy(nodeId, runId, progress);
                StateTransitions.validateNodeClaim(nodeId, current.doc(), next);
                String ineligible = whyCannotClaim(clusterService.state(), nodeId, next);
                if (ineligible != null) {
                    throw new IllegalStateException("node [" + nodeId + "] cannot claim shard task [" + next.id() + "]: " + ineligible);
                }
                write(next, current, l);
            });
        }

        /**
         * Real time read of one shard task.
         */
        public void getShardTask(ProjectId projectId, String id, ActionListener<Optional<Versioned<ShardTaskDoc>>> listener) {
            get(projectId, id, ShardTaskDoc::fromXContent, listener);
        }

        /**
         * Writes a shard task this node owns.
         *
         * @param heldEpoch the epoch the local run was started with
         */
        public void updateShardTask(
            Versioned<ShardTaskDoc> current,
            ShardTaskDoc next,
            long heldEpoch,
            ActionListener<Versioned<ShardTaskDoc>> listener
        ) {
            ActionListener.run(listener, l -> {
                ensureSameDoc(current.doc(), next);
                StateTransitions.validateNodeShardTaskWrite(nodeId, heldEpoch, current.doc(), next);
                if (targetIndexExists(next.projectId(), next.targetIndex(), next.targetIndexUuid()) == false) {
                    throw new IllegalStateException(
                        "target index ["
                            + next.targetIndex()
                            + "] with uuid ["
                            + next.targetIndexUuid()
                            + "] does not exist in project ["
                            + next.projectId()
                            + "], shard task ["
                            + next.id()
                            + "] cannot be written"
                    );
                }
                write(next, current, l);
            });
        }

        private boolean targetIndexExists(ProjectId projectId, String name, String uuid) {
            return DownsampleStateStore.targetIndexExists(clusterService.state(), projectId, name, uuid);
        }
    }

    /**
     * Why the given node cannot claim the given shard task in this cluster state, or {@code null} if it can.
     */
    @Nullable
    public static String whyCannotClaim(ClusterState state, String nodeId, ShardTaskDoc task) {
        if (state.metadata().nodeShutdowns().contains(nodeId)) {
            return "the node is marked for shutdown";
        }
        if (targetIndexExists(state, task.projectId(), task.targetIndex(), task.targetIndexUuid()) == false) {
            return "target index [" + task.targetIndex() + "] with uuid [" + task.targetIndexUuid() + "] does not exist";
        }
        if (state.metadata().projects().containsKey(task.projectId()) == false) {
            return "project [" + task.projectId() + "] does not exist";
        }
        IndexRoutingTable sourceRouting = state.routingTable(task.projectId()).index(task.sourceShardId().getIndex());
        if (sourceRouting == null) {
            return "source index [" + task.sourceShardId().getIndex() + "] does not exist";
        }
        for (ShardRouting copy : sourceRouting.shard(task.shard()).activeShards()) {
            if (nodeId.equals(copy.currentNodeId()) && copy.started() && copy.isSearchable()) {
                return null;
            }
        }
        return "the node does not hold a started, searchable copy of [" + task.sourceShardId() + "]";
    }

    /**
     * The uuids of the indices this node holds a started, searchable shard copy of: the source indices whose shard tasks it can
     * claim.
     */
    public static Set<String> locallySearchableIndexUuids(ClusterState state, String nodeId) {
        RoutingNode node = state.getRoutingNodes().node(nodeId);
        if (node == null) {
            return Set.of();
        }
        Set<String> uuids = new HashSet<>();
        for (ShardRouting copy : node) {
            if (copy.started() && copy.isSearchable()) {
                uuids.add(copy.index().getUUID());
            }
        }
        return uuids;
    }

    private static boolean targetIndexExists(ClusterState state, ProjectId projectId, String name, String uuid) {
        ProjectMetadata project = state.metadata().projects().get(projectId);
        IndexMetadata indexMetadata = project == null ? null : project.index(name);
        return indexMetadata != null && indexMetadata.getIndexUUID().equals(uuid);
    }

    private static void ensureSameDoc(StateDoc current, StateDoc next) {
        if (current.projectId().equals(next.projectId()) == false || current.id().equals(next.id()) == false) {
            throw new IllegalStateException(
                "cannot write document ["
                    + next.id()
                    + "] of project ["
                    + next.projectId()
                    + "] over document ["
                    + current.id()
                    + "] of project ["
                    + current.projectId()
                    + "]"
            );
        }
    }

    /**
     * Sends a request to the state index of the given project, whatever the project of the caller.
     */
    private <T> void inProject(ProjectId projectId, ActionListener<T> listener, Consumer<ActionListener<T>> request) {
        ProjectContext.run(client.threadPool().getThreadContext(), projectResolver, projectId, listener, request);
    }

    private <T extends StateDoc> void get(
        ProjectId projectId,
        String id,
        CheckedFunction<XContentParser, T, IOException> parser,
        ActionListener<Optional<Versioned<T>>> listener
    ) {
        GetRequest request = new GetRequest(INDEX_NAME, id).realtime(true);
        inProject(projectId, ActionListener.<GetResponse>wrap(response -> {
            if (response.isExists() == false) {
                listener.onResponse(Optional.empty());
            } else {
                listener.onResponse(
                    Optional.of(
                        versioned(projectId, response.getSourceAsBytesRef(), parser, response.getSeqNo(), response.getPrimaryTerm())
                    )
                );
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(Optional.empty());
            } else {
                listener.onFailure(e);
            }
        }), l -> client.get(request, l));
    }

    /**
     * Searches all the matching documents, failing if there are more than {@link #MAX_SEARCH_HITS}.
     */
    private <T extends StateDoc> void search(
        ProjectId projectId,
        BoolQueryBuilder query,
        CheckedFunction<XContentParser, T, IOException> parser,
        ActionListener<List<Versioned<T>>> listener
    ) {
        search(projectId, new SearchSourceBuilder().query(query).size(MAX_SEARCH_HITS).trackTotalHits(true), true, parser, listener);
    }

    /**
     * Searches the first {@code source.size()} matching documents.
     *
     * @param failOnMore whether to fail if more documents match, for callers that need all of them
     */
    private <T extends StateDoc> void search(
        ProjectId projectId,
        SearchSourceBuilder source,
        boolean failOnMore,
        CheckedFunction<XContentParser, T, IOException> parser,
        ActionListener<List<Versioned<T>>> listener
    ) {
        SearchRequest request = new SearchRequest(INDEX_NAME).source(source.seqNoAndPrimaryTerm(true));
        inProject(projectId, ActionListener.<SearchResponse>wrap(response -> {
            if (failOnMore && response.getHits().getTotalHits() != null && response.getHits().getTotalHits().value() > MAX_SEARCH_HITS) {
                throw new IllegalStateException(
                    "found more than [" + MAX_SEARCH_HITS + "] downsample state documents, pagination is not supported yet"
                );
            }
            List<Versioned<T>> docs = new ArrayList<>(response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                docs.add(versioned(projectId, hit.getSourceRef(), parser, hit.getSeqNo(), hit.getPrimaryTerm()));
            }
            listener.onResponse(docs);
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(List.of());
            } else {
                listener.onFailure(e);
            }
        }), l -> client.search(request, l));
    }

    /**
     * Creates the document in the state index of its project when {@code current} is {@code null}, otherwise replaces it only if
     * it is still at the sequence number and primary term of {@code current}.
     */
    private <T extends StateDoc> void write(T doc, @Nullable Versioned<T> current, ActionListener<Versioned<T>> listener)
        throws IOException {
        logger.trace("writing [{}] of project [{}] (create: [{}])", doc.id(), doc.projectId(), current == null);
        IndexRequest request = new IndexRequest(INDEX_NAME).id(doc.id())
            .source(doc.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        if (current == null) {
            request.opType(DocWriteRequest.OpType.CREATE);
        } else {
            request.setIfSeqNo(current.seqNo()).setIfPrimaryTerm(current.primaryTerm());
        }
        inProject(
            doc.projectId(),
            listener.<DocWriteResponse>map(response -> new Versioned<>(doc, response.getSeqNo(), response.getPrimaryTerm())),
            l -> client.index(request, l)
        );
    }

    /**
     * Parses a document read from the state index of {@code projectId}, checking that it belongs to that project.
     */
    private static <T extends StateDoc> Versioned<T> versioned(
        ProjectId projectId,
        BytesReference source,
        CheckedFunction<XContentParser, T, IOException> parser,
        long seqNo,
        long primaryTerm
    ) throws IOException {
        T doc;
        try (XContentParser xContentParser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, XContentType.JSON)) {
            doc = parser.apply(xContentParser);
        }
        if (doc.projectId().equals(projectId) == false) {
            throw new IllegalStateException(
                "document [" + doc.id() + "] of project [" + doc.projectId() + "] found in the state index of project [" + projectId + "]"
            );
        }
        return new Versioned<>(doc, seqNo, primaryTerm);
    }
}
