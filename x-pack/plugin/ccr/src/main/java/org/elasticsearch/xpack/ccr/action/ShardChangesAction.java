/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ShardChangesAction extends Action<ShardChangesAction.Request, ShardChangesAction.Response, ShardChangesAction.RequestBuilder> {

    public static final ShardChangesAction INSTANCE = new ShardChangesAction();
    public static final String NAME = "cluster:admin/xpack/ccr/shard_changes";

    private ShardChangesAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends SingleShardRequest<Request> {

        private long minSeqNo;
        private long maxSeqNo;
        private ShardId shardId;
        private long maxTranslogsBytes = ShardFollowTasksExecutor.DEFAULT_MAX_TRANSLOG_BYTES;

        public Request(ShardId shardId) {
            super(shardId.getIndexName());
            this.shardId = shardId;
        }

        Request() {
        }

        public ShardId getShard() {
            return shardId;
        }

        public long getMinSeqNo() {
            return minSeqNo;
        }

        public void setMinSeqNo(long minSeqNo) {
            this.minSeqNo = minSeqNo;
        }

        public long getMaxSeqNo() {
            return maxSeqNo;
        }

        public void setMaxSeqNo(long maxSeqNo) {
            this.maxSeqNo = maxSeqNo;
        }

        public long getMaxTranslogsBytes() {
            return maxTranslogsBytes;
        }

        public void setMaxTranslogsBytes(long maxTranslogsBytes) {
            this.maxTranslogsBytes = maxTranslogsBytes;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (minSeqNo < 0) {
                validationException = addValidationError("minSeqNo [" + minSeqNo + "] cannot be lower than 0", validationException);
            }
            if (maxSeqNo < minSeqNo) {
                validationException = addValidationError("minSeqNo [" + minSeqNo + "] cannot be larger than maxSeqNo ["
                        + maxSeqNo +  "]", validationException);
            }
            if (maxTranslogsBytes <= 0) {
                validationException = addValidationError("maxTranslogsBytes [" + maxTranslogsBytes + "] must be larger than 0",
                        validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            minSeqNo = in.readVLong();
            maxSeqNo = in.readVLong();
            shardId = ShardId.readShardId(in);
            maxTranslogsBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(minSeqNo);
            out.writeVLong(maxSeqNo);
            shardId.writeTo(out);
            out.writeVLong(maxTranslogsBytes);
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return minSeqNo == request.minSeqNo &&
                    maxSeqNo == request.maxSeqNo &&
                    Objects.equals(shardId, request.shardId) &&
                    maxTranslogsBytes == request.maxTranslogsBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minSeqNo, maxSeqNo, shardId, maxTranslogsBytes);
        }
    }

    public static final class Response extends ActionResponse {

        private Translog.Operation[] operations;

        Response() {
        }

        Response(final Translog.Operation[] operations) {
            this.operations = operations;
        }

        public Translog.Operation[] getOperations() {
            return operations;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeArray(Translog.Operation::writeOperation, operations);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response response = (Response) o;
            return Arrays.equals(operations, response.operations);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(operations);
        }
    }

    static class RequestBuilder extends SingleShardOperationRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(Settings settings,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               IndicesService indicesService) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new, ThreadPool.Names.GET);
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexService indexService = indicesService.indexServiceSafe(request.getShard().getIndex());
            IndexShard indexShard = indexService.getShard(request.getShard().id());

            request.maxSeqNo = Math.min(request.maxSeqNo, indexShard.getGlobalCheckpoint());
            return getOperationsBetween(indexShard, request.minSeqNo, request.maxSeqNo, request.maxTranslogsBytes);
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state.routingTable()
                    .index(request.concreteIndex())
                    .shard(request.request().getShard().id())
                    .activeInitializingShardsRandomIt();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

    }

    private static final Translog.Operation[] EMPTY_OPERATIONS_ARRAY = new Translog.Operation[0];

    static Response getOperationsBetween(IndexShard indexShard, long minSeqNo, long maxSeqNo, long byteLimit) throws IOException {
        if (indexShard.state() != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());
        }

        // TODO: Somehow this needs to be an internal refresh (SearcherScope.INTERNAL)
        indexShard.refresh("shard_changes_api");
        // TODO: Somehow this needs to acquire an internal searcher (SearcherScope.INTERNAL)
        try (Engine.Searcher searcher = indexShard.acquireSearcher("shard_changes_api")) {
            List<Translog.Operation> operations = getOperationsBetween(minSeqNo, maxSeqNo, byteLimit,
                    searcher.getDirectoryReader(), indexShard.mapperService());
            return new Response(operations.toArray(EMPTY_OPERATIONS_ARRAY));
        }
    }

    static List<Translog.Operation> getOperationsBetween(long minSeqNo, long maxSeqNo, long byteLimit,
                                                         DirectoryReader indexReader, MapperService mapperService) throws IOException {
        IndexSearcher searcher = new IndexSearcher(new CCRIndexReader(indexReader));
        searcher.setQueryCache(null);

        MappedFieldType seqNoFieldType = mapperService.fullName(SeqNoFieldMapper.NAME);
        assert mapperService.types().size() == 1;
        String type = mapperService.types().iterator().next();

        int size = ((int) (maxSeqNo - minSeqNo)) + 1;
        Sort sort = new Sort(new SortedNumericSortField(seqNoFieldType.name(), SortField.Type.LONG));
        Query query;
        if (mapperService.hasNested()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(Queries.newNonNestedFilter(mapperService.getIndexSettings().getIndexVersionCreated()), Occur.FILTER);
            builder.add(seqNoFieldType.rangeQuery(minSeqNo, maxSeqNo, true, true, null, null, null, null), Occur.FILTER);
            query = builder.build();
        } else {
            query = seqNoFieldType.rangeQuery(minSeqNo, maxSeqNo, true, true, null, null, null, null);
        }
        TopDocs topDocs = searcher.search(query, size, sort);
        if (topDocs.scoreDocs.length != size) {
            String message = "not all operations between min_seq_no [" + minSeqNo + "] and max_seq_no [" + maxSeqNo + "] found";
            throw new IllegalStateException(message);
        }

        long seenBytes = 0;
        final List<Translog.Operation> operations = new ArrayList<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            FieldsVisitor fieldsVisitor = new FieldsVisitor(true);
            searcher.doc(scoreDoc.doc, fieldsVisitor);
            fieldsVisitor.postProcess(mapperService);

            String id = fieldsVisitor.uid().id();
            String routing = fieldsVisitor.routing();
            if (fieldsVisitor.source() == null) {
                throw new IllegalArgumentException("no source found for document with id [" + id + "]");
            }
            byte[] source = fieldsVisitor.source().toBytesRef().bytes;

            // TODO: optimize this so that we fetch doc values data in segment and doc id order (see: DocValueFieldsFetchSubPhase):
            int leafReaderIndex = ReaderUtil.subIndex(scoreDoc.doc, searcher.getIndexReader().leaves());
            LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(leafReaderIndex);
            int segmentDocId = scoreDoc.doc - leafReaderContext.docBase;
            long version = readNumberDvValue(leafReaderContext, VersionFieldMapper.NAME, segmentDocId);
            long seqNo = readNumberDvValue(leafReaderContext, SeqNoFieldMapper.NAME, segmentDocId);
            long primaryTerm = readNumberDvValue(leafReaderContext, SeqNoFieldMapper.PRIMARY_TERM_NAME, segmentDocId);

            // TODO: handle NOOPs
            // NOTE: versionType can always be INTERNAL,
            //       because version logic has already been taken care of when indexing into leader shard.
            final VersionType versionType = VersionType.INTERNAL;
            final Translog.Operation op;
            if (isDeleteOperation(leafReaderContext, segmentDocId)) {
                BytesRef idBytes = Uid.encodeId(id);
                Term uidForDelete = new Term(IdFieldMapper.NAME, idBytes);
                op = new Translog.Delete(type, id, uidForDelete, seqNo, primaryTerm, version, versionType);
            } else {
                // NOTE: autoGeneratedIdTimestamp can always be -1,
                //       because auto id generation has already been performed when inxdexing into leader shard.
                final int autoGeneratedId = -1;
                op = new Translog.Index(type, id, seqNo, primaryTerm, version, versionType, source, routing, autoGeneratedId);
            }
            seenBytes += op.estimateSize();
            operations.add(op);
            if (seenBytes > byteLimit) {
                return operations;
            }
        }
        return operations;
    }

    private static long readNumberDvValue(LeafReaderContext leafReaderContext, String fieldName, int segmentDocId) throws IOException {
        NumericDocValues versionDvField = leafReaderContext.reader().getNumericDocValues(fieldName);
        assert versionDvField != null : fieldName + " field is missing";
        boolean advanced = versionDvField.advanceExact(segmentDocId);
        assert advanced;
        return versionDvField.longValue();
    }

    private static boolean isDeleteOperation(LeafReaderContext leafReaderContext, int segmentDocId) throws IOException {
        NumericDocValues softDeleteField = leafReaderContext.reader().getNumericDocValues(Lucene.SOFT_DELETE_FIELD);
        if (softDeleteField == null) {
            return false;
        }

        boolean advanced = softDeleteField.advanceExact(segmentDocId);
        if (advanced == false) {
            return false;
        }

        long value = softDeleteField.longValue();
        return value == 1L;
    }

    static final class CCRIndexReader extends FilterDirectoryReader {

        static final class CCRSubReaderWrapper extends SubReaderWrapper {
            @Override
            public LeafReader wrap(LeafReader in) {
                return new FilterLeafReader(in) {
                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return null;
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return null;
                    }

                    @Override
                    public int numDocs() {
                        return maxDoc();
                    }

                    @Override
                    public Bits getLiveDocs() {
                        return null;
                    }
                };
            }
        }

        CCRIndexReader(DirectoryReader in) throws IOException {
            super(in, new CCRSubReaderWrapper());
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new CCRIndexReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

}
