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
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.ROLLUP_ORIGIN;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.DOC_TYPE_FIELD;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.INDEX_NAME;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.INDEX_TASK_DOC_TYPE;
import static org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex.SHARD_TASK_DOC_TYPE;

/**
 * The only way to read and write {@link DownsampleStateIndex}. Access is split by role so that a caller can only reach the
 * operations its role allows: {@link #forMaster()} for the orchestration on the elected master, {@link #forLocalNode()} for
 * the node that runs shard tasks. Every write is validated with {@link StateTransitions} and sent as a compare-and-set
 * (a create, or an index with {@code if_seq_no} / {@code if_primary_term}), so a concurrent change makes the write fail with a
 * {@link VersionConflictEngineException} instead of being overwritten; the caller then re-reads and re-decides.
 * <p>
 * All requests use the {@link org.elasticsearch.xpack.core.ClientHelper#ROLLUP_ORIGIN} origin, which is also the origin of
 * the system index descriptor. Searches are near real time; gets are real time.
 * <p>
 * The enforcement is in code: it protects against bugs and stale actors (an old master, a node that lost its task), not
 * against a compromised node.
 */
public class DownsampleStateStore {

    /**
     * Searches are not paginated in the proof of concept, so they fail instead of silently returning a partial result.
     */
    static final int MAX_SEARCH_HITS = 10_000;

    private final Client client;
    private final ClusterService clusterService;

    public DownsampleStateStore(Client client, ClusterService clusterService) {
        this.client = new OriginSettingClient(client, ROLLUP_ORIGIN);
        this.clusterService = clusterService;
    }

    /**
     * A document with the sequence number and primary term it was read or written at, needed for the next compare-and-set.
     */
    public record Versioned<T>(T doc, long seqNo, long primaryTerm) {}

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
            ActionListener.run(listener, l -> {
                ensureMaster();
                get(doc.id(), IndexTaskDoc::fromXContent, l.delegateFailureAndWrap((delegate, existing) -> {
                    if (existing.isPresent() && existing.get().doc().phase().isTerminal() == false) {
                        delegate.onResponse(existing.get());
                        return;
                    }
                    Versioned<IndexTaskDoc> current = existing.orElse(null);
                    StateTransitions.validateMasterIndexTaskWrite(current == null ? null : current.doc(), doc);
                    write(doc.id(), doc, current, delegate);
                }));
            });
        }

        public void getIndexTask(String targetIndex, ActionListener<Optional<Versioned<IndexTaskDoc>>> listener) {
            get(DownsampleStateIndex.indexTaskId(targetIndex), IndexTaskDoc::fromXContent, listener);
        }

        public void updateIndexTask(Versioned<IndexTaskDoc> current, IndexTaskDoc next, ActionListener<Versioned<IndexTaskDoc>> listener) {
            ActionListener.run(listener, l -> {
                ensureMaster();
                ensureSameId(current.doc().id(), next.id());
                StateTransitions.validateMasterIndexTaskWrite(current.doc(), next);
                write(next.id(), next, current, l);
            });
        }

        /**
         * All the operations that are not finished.
         */
        public void findActiveIndexTasks(ActionListener<List<Versioned<IndexTaskDoc>>> listener) {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, INDEX_TASK_DOC_TYPE))
                .mustNot(
                    QueryBuilders.termsQuery(
                        IndexTaskDoc.PHASE.getPreferredName(),
                        IndexTaskPhase.DONE.name(),
                        IndexTaskPhase.FAILED.name()
                    )
                );
            search(query, IndexTaskDoc::fromXContent, listener);
        }

        /**
         * Creates a shard task document. If it already exists (e.g. a new master re-running the start of the shard phase), the
         * existing document is returned unchanged.
         */
        public void createShardTask(ShardTaskDoc doc, ActionListener<Versioned<ShardTaskDoc>> listener) {
            ActionListener.run(listener, l -> {
                ensureMaster();
                StateTransitions.validateMasterShardTaskWrite(null, doc);
                write(doc.id(), doc, null, ActionListener.wrap(l::onResponse, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        get(doc.id(), ShardTaskDoc::fromXContent, l.map(existing -> existing.orElseThrow(() -> e)));
                    } else {
                        l.onFailure(e);
                    }
                }));
            });
        }

        /**
         * All the shard tasks of one operation.
         */
        public void findShardTasks(String targetIndex, String targetIndexUuid, ActionListener<List<Versioned<ShardTaskDoc>>> listener) {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, SHARD_TASK_DOC_TYPE))
                .filter(QueryBuilders.termQuery(IndexTaskDoc.TARGET_INDEX.getPreferredName(), targetIndex))
                .filter(QueryBuilders.termQuery(IndexTaskDoc.TARGET_INDEX_UUID.getPreferredName(), targetIndexUuid));
            search(query, ShardTaskDoc::fromXContent, listener);
        }

        public void updateShardTask(Versioned<ShardTaskDoc> current, ShardTaskDoc next, ActionListener<Versioned<ShardTaskDoc>> listener) {
            ActionListener.run(listener, l -> {
                ensureMaster();
                ensureSameId(current.doc().id(), next.id());
                StateTransitions.validateMasterShardTaskWrite(current.doc(), next);
                write(next.id(), next, current, l);
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
         * The shard tasks this node currently owns. Near real time: a recent assignment can be missing, a recent reassignment
         * can still show up, so confirm with {@link #getShardTask} before acting on a removal.
         */
        public void findMyShardTasks(ActionListener<List<Versioned<ShardTaskDoc>>> listener) {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DOC_TYPE_FIELD, SHARD_TASK_DOC_TYPE))
                .filter(QueryBuilders.termQuery(ShardTaskDoc.ASSIGNED_NODE.getPreferredName(), nodeId))
                .filter(
                    QueryBuilders.termsQuery(
                        ShardTaskDoc.STATUS.getPreferredName(),
                        ShardTaskStatus.ASSIGNED.name(),
                        ShardTaskStatus.RUNNING.name()
                    )
                );
            search(query, ShardTaskDoc::fromXContent, listener);
        }

        /**
         * Real time read of one shard task.
         */
        public void getShardTask(String id, ActionListener<Optional<Versioned<ShardTaskDoc>>> listener) {
            get(id, ShardTaskDoc::fromXContent, listener);
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
                ensureSameId(current.doc().id(), next.id());
                StateTransitions.validateNodeShardTaskWrite(nodeId, heldEpoch, current.doc(), next);
                if (targetIndexExists(next.targetIndex(), next.targetIndexUuid()) == false) {
                    throw new IllegalStateException(
                        "target index ["
                            + next.targetIndex()
                            + "] with uuid ["
                            + next.targetIndexUuid()
                            + "] does not exist, shard task ["
                            + next.id()
                            + "] cannot be written"
                    );
                }
                write(next.id(), next, current, l);
            });
        }

        private boolean targetIndexExists(String name, String uuid) {
            for (ProjectMetadata project : clusterService.state().metadata().projects().values()) {
                IndexMetadata indexMetadata = project.index(name);
                if (indexMetadata != null && indexMetadata.getIndexUUID().equals(uuid)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static void ensureSameId(String currentId, String nextId) {
        if (currentId.equals(nextId) == false) {
            throw new IllegalStateException("cannot write document [" + nextId + "] over document [" + currentId + "]");
        }
    }

    private <T> void get(
        String id,
        CheckedFunction<XContentParser, T, IOException> parser,
        ActionListener<Optional<Versioned<T>>> listener
    ) {
        client.get(new GetRequest(INDEX_NAME, id).realtime(true), ActionListener.wrap(response -> {
            if (response.isExists() == false) {
                listener.onResponse(Optional.empty());
            } else {
                T doc = parse(response.getSourceAsBytesRef(), parser);
                listener.onResponse(Optional.of(new Versioned<>(doc, response.getSeqNo(), response.getPrimaryTerm())));
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(Optional.empty());
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private <T> void search(
        BoolQueryBuilder query,
        CheckedFunction<XContentParser, T, IOException> parser,
        ActionListener<List<Versioned<T>>> listener
    ) {
        SearchRequest request = new SearchRequest(INDEX_NAME).source(
            new SearchSourceBuilder().query(query).size(MAX_SEARCH_HITS).seqNoAndPrimaryTerm(true).trackTotalHits(true)
        );
        client.search(request, ActionListener.wrap(response -> {
            if (response.getHits().getTotalHits() != null && response.getHits().getTotalHits().value() > MAX_SEARCH_HITS) {
                throw new IllegalStateException(
                    "found more than [" + MAX_SEARCH_HITS + "] downsample state documents, pagination is not supported yet"
                );
            }
            List<Versioned<T>> docs = new ArrayList<>(response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                docs.add(new Versioned<>(parse(hit.getSourceRef(), parser), hit.getSeqNo(), hit.getPrimaryTerm()));
            }
            listener.onResponse(docs);
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(List.of());
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Creates the document when {@code current} is {@code null}, otherwise replaces it only if it is still at the sequence
     * number and primary term of {@code current}.
     */
    private <T extends ToXContentObject> void write(String id, T doc, @Nullable Versioned<T> current, ActionListener<Versioned<T>> listener)
        throws IOException {
        IndexRequest request = new IndexRequest(INDEX_NAME).id(id)
            .source(doc.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        if (current == null) {
            request.opType(DocWriteRequest.OpType.CREATE);
        } else {
            request.setIfSeqNo(current.seqNo()).setIfPrimaryTerm(current.primaryTerm());
        }
        client.index(request, listener.map(response -> new Versioned<>(doc, response.getSeqNo(), response.getPrimaryTerm())));
    }

    private static <T> T parse(BytesReference source, CheckedFunction<XContentParser, T, IOException> parser) throws IOException {
        try (XContentParser xContentParser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, XContentType.JSON)) {
            return parser.apply(xContentParser);
        }
    }
}
