/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.lucene.queries.BinaryDocValuesQueries;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor;
import org.elasticsearch.xpack.esql.streaming.StreamMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_OFFSET_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_TRANSPORT_VERSION_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_ID_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_STATUS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOTAL_PAGES;

/**
 * Reads consecutive pages of one stored result straight from the shard holding it, up to a byte budget. Pages are returned as
 * they were stored, in page order, and are decoded by the caller. Their bytes are charged to the request breaker until the
 * response is released. Stateless clusters read from the indexing shard, others from any copy.
 * <p>
 * A read can wait for its first page, through {@link StoredPagesService}, unless the result has ended before it. The index is
 * never refreshed on a schedule.
 */
public class TransportReadStreamPagesAction extends TransportSingleShardAction<
    TransportReadStreamPagesAction.Request,
    TransportReadStreamPagesAction.Response> {

    public static final String NAME = "indices:data/read/esql/stream/pages";
    public static final ActionType<Response> TYPE = new ActionType<>(NAME);

    static final String REFRESH_SOURCE = "esql_stored_result_read";
    static final String BREAKER_LABEL = "esql_stored_result_pages";
    static final int MAX_PAGES_PER_READ = 1024;
    static final TimeValue MAX_WAIT_TIMEOUT = TimeValue.timeValueMinutes(1);
    /**
     * The seq of the metadata document of a result; its pages of rows start at {@code 1}.
     */
    static final int METADATA_SEQ = 0;

    private final IndicesService indicesService;
    private final StoredPagesService service;
    private final CircuitBreaker breaker;
    private final Executor executor;
    private final boolean stateless;

    @Inject
    public TransportReadStreamPagesAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        StoredPagesService service,
        CircuitBreakerService circuitBreakerService,
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
        this.service = service;
        this.breaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        this.executor = threadPool.executor(ThreadPool.Names.GET);
        this.stateless = DiscoveryNode.isStateless(clusterService.getSettings());
    }

    @Override
    protected Writeable.Reader<Response> getResponseReader() {
        return in -> new Response(in, breaker);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        super.doExecute(task, request, failOnTrip(listener));
    }

    /**
     * Fails with the breaker trip of a response read on this node, rather than reading the same pages from another copy.
     */
    static ActionListener<Response> failOnTrip(ActionListener<Response> listener) {
        return listener.delegateFailureAndWrap((l, response) -> {
            if (response.tripped != null) {
                l.onFailure(response.tripped);
            } else {
                l.onResponse(response);
            }
        });
    }

    @Override
    protected boolean resolveIndex(Request request) {
        return false;
    }

    @Override
    protected ShardIterator shards(ProjectState project, InternalRequest request) {
        String streamId = request.request().streamId;
        // the pages of a result are routed by its id
        int shard = OperationRouting.shardId(project.metadata(), request.concreteIndex(), streamId, streamId).id();
        if (stateless) {
            // the indexing shard: search shards only see pages once they are uploaded
            return project.routingTable().shardRoutingTable(request.concreteIndex(), shard).primaryShardIt();
        }
        // any copy holds the acknowledged pages; stick to one copy per result. The prefix keeps an id starting with '_' from
        // being read as a named preference.
        return clusterService.operationRouting().getShards(project, request.concreteIndex(), shard, "result:" + streamId);
    }

    @Override
    protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) {
        executor.execute(
            ActionRunnable.wrap(
                listener,
                l -> service.read(shardId, request.streamId, () -> shardOperation(request, shardId), request.waitTimeout, l)
            )
        );
    }

    /**
     * Reads pages from {@code fromSeq} on until one is missing, {@code maxPages} are read, or the next would go over
     * {@code maxBytes}; the first page is always read. A page that trips the breaker ends the read, or fails it if it is the first.
     * A read that finds no page tells whether the result ended before it.
     */
    @Override
    protected Response shardOperation(Request request, ShardId shardId) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard shard = indexService.getShard(shardId.id());
        try (Engine.Searcher searcher = shard.acquireSearcher("esql_stream_results")) {
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
            PageReader reader = new PageReader(context);
            List<Run> runs = runs(searcher, context, request.streamId);
            int fromSeq = request.fromRow >= 0 ? seqForRow(reader, runs, request.fromRow) : request.fromSeq;
            int toSeq = (int) Math.min((long) fromSeq + request.maxPages - 1, Integer.MAX_VALUE);
            List<PageDoc> docs = findPages(reader, runs, fromSeq, toSeq);
            Response response = readPages(searcher, reader, docs, fromSeq, request.maxBytes);
            if (response.pages.isEmpty() && endsBefore(searcher, context, reader, runs, fromSeq)) {
                response.decRef();
                return new Response(List.of(), true, () -> {});
            }
            return response;
        }
    }

    /**
     * The documents of a result in each leaf. The index is sorted by result and page, so in a leaf they are one run of
     * documents in page order, the metadata document first. The run is found by bisecting the column of result ids, and a
     * position inside it by bisecting the page fields, so a read never visits the documents it does not return.
     */
    private record Run(LeafReaderContext leaf, int from, int to) {}

    private static List<Run> runs(Engine.Searcher searcher, SearchExecutionContext context, String streamId) throws IOException {
        // built from the doc values format rather than the field type, which refuses doc-values-only queries when expensive
        // queries are disabled
        KeywordFieldMapper.KeywordFieldType idField = (KeywordFieldMapper.KeywordFieldType) context.getFieldType(STREAM_ID_FIELD);
        Query query = BinaryDocValuesQueries.forFormat(idField.binaryFormat()).term(STREAM_ID_FIELD, new BytesRef(streamId));
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<Run> runs = new ArrayList<>();
        for (LeafReaderContext leaf : searcher.getLeafContexts()) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            DocIdSetIterator it = scorer.iterator();
            int from = it.nextDoc();
            if (from == DocIdSetIterator.NO_MORE_DOCS) {
                continue;
            }
            int to = it.docIDRunEnd();
            while (it.advance(to) == to) {
                to = it.docIDRunEnd();
            }
            runs.add(new Run(leaf, from, to));
        }
        return runs;
    }

    /**
     * The first document of {@code run} from {@code from} on whose value of {@code field} is above {@code bound}, or the end of the
     * run. The value must not decrease along the run.
     */
    private static int firstAbove(IndexNumericFieldData field, Run run, int from, long bound) throws IOException {
        int lo = from;
        int hi = run.to;
        SortedNumericLongValues values = null;
        int probed = Integer.MAX_VALUE;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (mid < probed) {
                // doc values only move forward
                values = field.load(run.leaf).getLongValues();
            }
            probed = mid;
            if (value(values, mid) <= bound) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * The live documents of the pages from {@code fromSeq} to {@code toSeq}, in page order, without their content.
     */
    private static List<PageDoc> findPages(PageReader reader, List<Run> runs, int fromSeq, int toSeq) throws IOException {
        List<PageDoc> docs = new ArrayList<>();
        for (Run run : runs) {
            Bits live = run.leaf.reader().getLiveDocs();
            SortedNumericLongValues seq = reader.seq.load(run.leaf).getLongValues();
            for (int doc = firstAbove(reader.seq, run, run.from, fromSeq - 1L); doc < run.to; doc++) {
                long s = value(seq, doc);
                if (s > toSeq) {
                    break;
                }
                if (live == null || live.get(doc)) {
                    docs.add(new PageDoc((int) s, run.leaf.ord, doc));
                }
            }
        }
        docs.sort(Comparator.comparingInt(PageDoc::seq));
        return docs;
    }

    /**
     * Whether the result ended before page {@code fromSeq}: its metadata document says the query is done, with fewer pages when
     * it says how many. The last bulk of a result refreshes its shard, so a reader that sees the end sees every page before it.
     */
    private static boolean endsBefore(
        Engine.Searcher searcher,
        SearchExecutionContext context,
        PageReader reader,
        List<Run> runs,
        int fromSeq
    ) throws IOException {
        List<PageDoc> docs = findPages(reader, runs, METADATA_SEQ, METADATA_SEQ);
        if (docs.isEmpty()) {
            return false;
        }
        PageDoc doc = docs.getFirst();
        LeafReaderContext leaf = searcher.getLeafContexts().get(doc.leaf);
        SortableBinaryDocValues status = context.getForField(context.getFieldType(STREAM_STATUS), MappedFieldType.FielddataOperation.SEARCH)
            .load(leaf)
            .getBytesValues();
        if (status.advanceExact(doc.doc) == false
            || StreamMetadata.Status.valueOf(status.nextValue().utf8ToString()) == StreamMetadata.Status.RUNNING) {
            return false;
        }
        SortedNumericLongValues pages = PageReader.numeric(context, STREAM_TOTAL_PAGES).load(leaf).getLongValues();
        return pages.advanceExact(doc.doc) == false || fromSeq > pages.nextValue();
    }

    private Response readPages(Engine.Searcher searcher, PageReader reader, List<PageDoc> docs, int fromSeq, long maxBytes)
        throws IOException {
        List<StoredPage> pages = new ArrayList<>();
        long charged = 0;
        try {
            // one leaf's pages come in doc order, as the index is sorted by page
            Map<Integer, LeafPageReader> leaves = new HashMap<>();
            for (PageDoc doc : docs) {
                if (doc.seq != fromSeq + pages.size()) {
                    break;
                }
                LeafPageReader leaf = leaves.computeIfAbsent(doc.leaf, l -> new LeafPageReader(reader, searcher.getLeafContexts().get(l)));
                BytesRef page = leaf.page(doc.doc);
                if (pages.isEmpty() == false && charged + page.length > maxBytes) {
                    break;
                }
                try {
                    breaker.addEstimateBytesAndMaybeBreak(page.length, BREAKER_LABEL);
                } catch (CircuitBreakingException e) {
                    if (pages.isEmpty()) {
                        throw e;
                    }
                    break;
                }
                charged += page.length;
                pages.add(leaf.storedPage(doc, page));
            }
        } catch (Exception e) {
            breaker.addWithoutBreaking(-charged);
            throw e;
        }
        long toRelease = charged;
        return new Response(pages, false, () -> breaker.addWithoutBreaking(-toRelease));
    }

    /**
     * The page that holds {@code row}, or the page that will hold it if it is not stored yet. A deleted copy of a page holds the
     * same position as the live one, so liveness does not matter here.
     */
    private static int seqForRow(PageReader reader, List<Run> runs, long row) throws IOException {
        // the last page starting at or before the row
        long seq = 0;
        long firstRow = -1;
        long rowCount = 0;
        for (Run run : runs) {
            int pagesFrom = firstAbove(reader.seq, run, run.from, METADATA_SEQ);
            int doc = firstAbove(reader.firstRow, run, pagesFrom, row) - 1;
            if (doc < pagesFrom) {
                continue;
            }
            long s = value(reader.seq.load(run.leaf).getLongValues(), doc);
            if (s > seq) {
                seq = s;
                firstRow = value(reader.firstRow.load(run.leaf).getLongValues(), doc);
                rowCount = value(reader.rowCount.load(run.leaf).getLongValues(), doc);
            }
        }
        if (seq == 0) {
            return 1;
        }
        return row < firstRow + rowCount ? (int) seq : (int) seq + 1;
    }

    /**
     * Doc-values readers for the fields of a page document.
     */
    private record PageReader(
        IndexNumericFieldData seq,
        IndexNumericFieldData firstRow,
        IndexNumericFieldData rowCount,
        IndexNumericFieldData transportVersion,
        IndexFieldData<?> page
    ) {
        PageReader(SearchExecutionContext context) {
            this(
                numeric(context, PAGE_SEQ_FIELD),
                numeric(context, PAGE_OFFSET_FIELD),
                numeric(context, PAGE_SIZE_FIELD),
                numeric(context, PAGE_TRANSPORT_VERSION_FIELD),
                context.getForField(context.getFieldType(PAGE_FIELD), MappedFieldType.FielddataOperation.SEARCH)
            );
        }

        private static IndexNumericFieldData numeric(SearchExecutionContext context, String field) {
            return context.getForField(context.getFieldType(field), MappedFieldType.FielddataOperation.SEARCH);
        }
    }

    /**
     * A page document, found before its content is read.
     */
    private record PageDoc(int seq, int leaf, int doc) {}

    /**
     * Reads the content of the pages of one leaf, in doc order.
     */
    private static class LeafPageReader {
        private final SortedNumericLongValues firstRow;
        private final SortedNumericLongValues rowCount;
        private final SortedNumericLongValues transportVersion;
        private final SortableBinaryDocValues page;

        LeafPageReader(PageReader reader, LeafReaderContext context) {
            firstRow = reader.firstRow.load(context).getLongValues();
            rowCount = reader.rowCount.load(context).getLongValues();
            transportVersion = reader.transportVersion.load(context).getLongValues();
            page = reader.page.load(context).getBytesValues();
        }

        BytesRef page(int doc) throws IOException {
            return page.advanceExact(doc) ? page.nextValue() : new BytesRef();
        }

        StoredPage storedPage(PageDoc doc, BytesRef bytes) throws IOException {
            return new StoredPage(
                doc.seq,
                value(firstRow, doc.doc),
                (int) value(rowCount, doc.doc),
                TransportVersion.fromId((int) value(transportVersion, doc.doc)),
                new BytesArray(BytesRef.deepCopyOf(bytes))
            );
        }
    }

    private static long value(SortedNumericLongValues values, int doc) throws IOException {
        if (values.advanceExact(doc) == false) {
            throw new IllegalStateException("page document [" + doc + "] is missing a value");
        }
        return values.nextValue();
    }

    /**
     * One page of rows as it was stored: its position in the result and its serialized content.
     */
    public record StoredPage(int seq, long firstRow, int rowCount, TransportVersion transportVersion, BytesReference bytes)
        implements
            Writeable {

        StoredPage(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVLong(), in.readVInt(), TransportVersion.readVersion(in), in.readBytesReference());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(seq);
            out.writeVLong(firstRow);
            out.writeVInt(rowCount);
            TransportVersion.writeVersion(transportVersion, out);
            out.writeBytesReference(bytes);
        }
    }

    /**
     * A read of consecutive pages of one result, from a page or from the page that holds a row.
     */
    public static class Request extends SingleShardRequest<Request> {
        private final String streamId;
        private final int fromSeq;
        private final long fromRow;
        private final int maxPages;
        private final long maxBytes;
        private final TimeValue waitTimeout;

        /**
         * Reads what is visible, without waiting and whatever its size.
         */
        public Request(String streamId, int fromSeq, int maxPages) {
            this(streamId, fromSeq, maxPages, Long.MAX_VALUE, TimeValue.ZERO);
        }

        /**
         * Reads from the page that holds {@code row}, as {@link #Request(String, int, int, long, TimeValue)} does. The first page
         * returned can start before the row.
         */
        public static Request fromRow(String streamId, long row, int maxPages, long maxBytes, TimeValue waitTimeout) {
            return new Request(streamId, -1, row, maxPages, maxBytes, waitTimeout);
        }

        /**
         * Reads up to {@code maxPages} pages and {@code maxBytes} bytes, at least one page. Waits up to {@code waitTimeout} when
         * the first page is not stored yet. The response can still be empty: the wait ends at the timeout, or as soon as a
         * document of the result is indexed, which may not be one of these pages.
         */
        public Request(String streamId, int fromSeq, int maxPages, long maxBytes, TimeValue waitTimeout) {
            this(streamId, fromSeq, -1, maxPages, maxBytes, waitTimeout);
        }

        private Request(String streamId, int fromSeq, long fromRow, int maxPages, long maxBytes, TimeValue waitTimeout) {
            super(EsqlStreamResultsIndexDescriptor.INDEX);
            this.streamId = streamId;
            this.fromSeq = fromSeq;
            this.fromRow = fromRow;
            this.maxPages = maxPages;
            this.maxBytes = maxBytes;
            this.waitTimeout = waitTimeout;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.streamId = in.readString();
            this.fromSeq = Math.toIntExact(in.readZLong());
            this.fromRow = in.readZLong();
            this.maxPages = in.readVInt();
            this.maxBytes = in.readVLong();
            this.waitTimeout = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(streamId);
            out.writeZLong(fromSeq);
            out.writeZLong(fromRow);
            out.writeVInt(maxPages);
            out.writeVLong(maxBytes);
            out.writeTimeValue(waitTimeout);
        }

        String streamId() {
            return streamId;
        }

        int fromSeq() {
            return fromSeq;
        }

        long fromRow() {
            return fromRow;
        }

        int maxPages() {
            return maxPages;
        }

        long maxBytes() {
            return maxBytes;
        }

        TimeValue waitTimeout() {
            return waitTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (streamId == null) {
                e = addValidationError("[streamId] is required", e);
            }
            if (fromRow < 0 && fromSeq <= METADATA_SEQ) {
                e = addValidationError("[fromSeq] must be at least 1, or [fromRow] positive, got [" + fromSeq + "]", e);
            }
            if (maxPages < 1 || maxPages > MAX_PAGES_PER_READ) {
                e = addValidationError("[maxPages] must be between 1 and [" + MAX_PAGES_PER_READ + "], got [" + maxPages + "]", e);
            }
            if (maxBytes < 0) {
                e = addValidationError("[maxBytes] must be positive, got [" + maxBytes + "]", e);
            }
            if (waitTimeout.compareTo(MAX_WAIT_TIMEOUT) > 0) {
                e = addValidationError("[waitTimeout] must be at most [" + MAX_WAIT_TIMEOUT + "], got [" + waitTimeout + "]", e);
            }
            return e;
        }
    }

    /**
     * The pages read, or none and whether the result ended before them. On the shard, it holds the memory charged for the pages
     * until it is released.
     */
    public static class Response extends ActionResponse {
        private final List<StoredPage> pages;
        private final boolean resultEnded;
        @Nullable
        private final CircuitBreakingException tripped;
        private final RefCounted refCounted;

        Response(List<StoredPage> pages, boolean resultEnded, Releasable onRelease) {
            this.pages = pages;
            this.resultEnded = resultEnded;
            this.tripped = null;
            this.refCounted = LeakTracker.wrap(AbstractRefCounted.of(onRelease::close));
        }

        /**
         * Reads a response from another node, charging its pages to {@code breaker} of this node until it is released. If the
         * pages trip the breaker, the response holds the trip instead of the pages, for {@link #failOnTrip} to fail the read.
         */
        Response(StreamInput in, CircuitBreaker breaker) throws IOException {
            List<StoredPage> read = in.readCollectionAsList(StoredPage::new);
            this.resultEnded = in.readBoolean();
            long bytes = 0;
            for (StoredPage page : read) {
                bytes += page.bytes.length();
            }
            CircuitBreakingException trip = null;
            try {
                breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
            } catch (CircuitBreakingException e) {
                trip = e;
                read = List.of();
                bytes = 0;
            }
            this.pages = read;
            this.tripped = trip;
            long charged = bytes;
            this.refCounted = LeakTracker.wrap(AbstractRefCounted.of(() -> breaker.addWithoutBreaking(-charged)));
        }

        /**
         * Whether a read can answer with it rather than wait.
         */
        boolean answers() {
            return pages.isEmpty() == false || resultEnded;
        }

        public boolean resultEnded() {
            return resultEnded;
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }

        public List<StoredPage> pages() {
            return pages;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(pages);
            out.writeBoolean(resultEnded);
        }
    }
}
