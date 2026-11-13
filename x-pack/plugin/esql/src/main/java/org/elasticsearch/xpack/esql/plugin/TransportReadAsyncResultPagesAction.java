/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncResultPagesIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.ASYNC_ID_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.FIRST_ROW_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.ROW_COUNT_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.TRANSPORT_VERSION_FIELD;

/**
 * Reads a range of pages of one stored result straight from the shard holding it. Pages are returned
 * as they were stored, in page order, and are decoded by the caller. Stateless clusters read from the
 * indexing shard, others from any copy.
 */
public class TransportReadAsyncResultPagesAction extends TransportSingleShardAction<
    TransportReadAsyncResultPagesAction.Request,
    TransportReadAsyncResultPagesAction.Response> {

    public static final String NAME = "indices:data/read/esql/async/pages";
    public static final ActionType<Response> TYPE = new ActionType<>(NAME);

    private final IndicesService indicesService;
    private final boolean stateless;

    @Inject
    public TransportReadAsyncResultPagesAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            projectResolver,
            indexNameExpressionResolver,
            Request::new,
            threadPool.executor(ThreadPool.Names.GET)
        );
        this.indicesService = indicesService;
        this.stateless = DiscoveryNode.isStateless(clusterService.getSettings());
    }

    @Override
    protected Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    @Override
    protected boolean resolveIndex(Request request) {
        return false;
    }

    @Override
    protected ShardIterator shards(ProjectState project, InternalRequest request) {
        String asyncDocId = request.request().asyncDocId;
        // the pages of a result are routed by its id
        int shard = OperationRouting.shardId(project.metadata(), request.concreteIndex(), asyncDocId, asyncDocId).id();
        if (stateless) {
            // the indexing shard: search shards only see pages once they are uploaded
            return project.routingTable().shardRoutingTable(request.concreteIndex(), shard).primaryShardIt();
        }
        // any copy holds the acknowledged pages; stick to one copy per result. The prefix keeps an id starting with '_' from
        // being read as a named preference.
        return clusterService.operationRouting().getShards(project, request.concreteIndex(), shard, "result:" + asyncDocId);
    }

    @Override
    protected Response shardOperation(Request request, ShardId shardId) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard shard = indexService.getShard(shardId.id());
        try (Engine.Searcher searcher = shard.acquireSearcher("async_result_pages")) {
            SearchExecutionContext context = indexService.newSearchExecutionContext(
                shardId.id(),
                0,
                searcher,
                () -> 0L,
                null,
                Map.of(),
                null,
                null
            );
            MappedFieldType seqType = context.getFieldType(PAGE_SEQ_FIELD);
            Query query = new BooleanQuery.Builder().add(
                context.getFieldType(ASYNC_ID_FIELD).termQuery(request.asyncDocId, context),
                BooleanClause.Occur.FILTER
            )
                .add(
                    seqType.rangeQuery(request.fromSeq, request.fromSeq + request.maxPages - 1, true, true, null, null, null, context),
                    BooleanClause.Occur.FILTER
                )
                .build();
            PageReader reader = new PageReader(context);
            List<StoredPage> pages = searcher.search(query, new CollectorManager<PageCollector, List<StoredPage>>() {
                @Override
                public PageCollector newCollector() {
                    return new PageCollector(reader);
                }

                @Override
                public List<StoredPage> reduce(Collection<PageCollector> collectors) {
                    List<StoredPage> all = new ArrayList<>();
                    for (PageCollector collector : collectors) {
                        all.addAll(collector.pages);
                    }
                    all.sort(Comparator.comparingInt(StoredPage::seq));
                    return all;
                }
            });
            return new Response(pages);
        }
    }

    /**
     * Doc-values readers for the fields of a page document.
     */
    private record PageReader(
        IndexNumericFieldData seq,
        IndexNumericFieldData firstRow,
        IndexNumericFieldData rowCount,
        IndexNumericFieldData transportVersion,
        IndexNumericFieldData expirationTime,
        IndexFieldData<?> page
    ) {
        PageReader(SearchExecutionContext context) {
            this(
                numeric(context, PAGE_SEQ_FIELD),
                numeric(context, FIRST_ROW_FIELD),
                numeric(context, ROW_COUNT_FIELD),
                numeric(context, TRANSPORT_VERSION_FIELD),
                numeric(context, EXPIRATION_TIME_FIELD),
                context.getForField(context.getFieldType(PAGE_FIELD), MappedFieldType.FielddataOperation.SEARCH)
            );
        }

        private static IndexNumericFieldData numeric(SearchExecutionContext context, String field) {
            return context.getForField(context.getFieldType(field), MappedFieldType.FielddataOperation.SEARCH);
        }
    }

    private static class PageCollector extends SimpleCollector {
        private final PageReader reader;
        private final List<StoredPage> pages = new ArrayList<>();
        private SortedNumericLongValues seq;
        private SortedNumericLongValues firstRow;
        private SortedNumericLongValues rowCount;
        private SortedNumericLongValues transportVersion;
        private SortedNumericLongValues expirationTime;
        private SortableBinaryDocValues page;

        PageCollector(PageReader reader) {
            this.reader = reader;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) {
            seq = reader.seq.load(context).getLongValues();
            firstRow = reader.firstRow.load(context).getLongValues();
            rowCount = reader.rowCount.load(context).getLongValues();
            transportVersion = reader.transportVersion.load(context).getLongValues();
            expirationTime = reader.expirationTime.load(context).getLongValues();
            page = reader.page.load(context).getBytesValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            BytesRef bytes = page.advanceExact(doc) ? page.nextValue() : new BytesRef();
            pages.add(
                new StoredPage(
                    (int) value(seq, doc),
                    value(firstRow, doc),
                    (int) value(rowCount, doc),
                    TransportVersion.fromId((int) value(transportVersion, doc)),
                    expirationTime.advanceExact(doc) ? expirationTime.nextValue() : -1L,
                    new BytesArray(BytesRef.deepCopyOf(bytes))
                )
            );
        }

        private static long value(SortedNumericLongValues values, int doc) throws IOException {
            if (values.advanceExact(doc) == false) {
                throw new IllegalStateException("page document [" + doc + "] is missing a value");
            }
            return values.nextValue();
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    /**
     * One page as it was stored: its position in the result and its serialized content. Page {@code 0} holds the
     * metadata of the result and is the only one with an {@code expirationTime}, {@code -1} for the others.
     */
    public record StoredPage(
        int seq,
        long firstRow,
        int rowCount,
        TransportVersion transportVersion,
        long expirationTime,
        BytesReference bytes
    ) implements Writeable {

        StoredPage(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVLong(), in.readVInt(), TransportVersion.readVersion(in), in.readZLong(), in.readBytesReference());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(seq);
            out.writeVLong(firstRow);
            out.writeVInt(rowCount);
            TransportVersion.writeVersion(transportVersion, out);
            out.writeZLong(expirationTime);
            out.writeBytesReference(bytes);
        }
    }

    public static class Request extends SingleShardRequest<Request> {
        private final String asyncDocId;
        private final int fromSeq;
        private final int maxPages;

        public Request(String asyncDocId, int fromSeq, int maxPages) {
            super(AsyncResultPagesIndex.INDEX);
            this.asyncDocId = asyncDocId;
            this.fromSeq = fromSeq;
            this.maxPages = maxPages;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.asyncDocId = in.readString();
            this.fromSeq = in.readVInt();
            this.maxPages = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(asyncDocId);
            out.writeVInt(fromSeq);
            out.writeVInt(maxPages);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {
        private final List<StoredPage> pages;

        Response(List<StoredPage> pages) {
            this.pages = pages;
        }

        Response(StreamInput in) throws IOException {
            this.pages = in.readCollectionAsList(StoredPage::new);
        }

        public List<StoredPage> pages() {
            return pages;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(pages);
        }
    }
}
