/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.EscfBulkWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streams an {@code INTO} query's pages through one {@link EscfBulkWriter}. Each driver has
 * its own producer. The writer bounds the bulks in flight with the node's {@link EscfBulkService#defaultPolicy()} and
 * charges the query breaker and coordinating indexing pressure. Rows are written with {@code create} unless
 * {@link IntoOptions#collisions()} is {@link IntoOptions.Collisions#REPLACE}, which overwrites with {@code index}. A
 * {@code _id} column sets each document's id instead of being written to its source.
 * A row the destination refuses fails the run, unless {@link IntoOptions#keepGoing()} is set: then it is counted and the
 * run continues. A bulk request that fails as a whole always fails the run, as does a row whose failure is transient
 * (too many requests or service unavailable), because that row was not refused by the destination and dropping it would
 * silently lose data. A row whose {@code _id} already exists is handled by the {@linkplain IntoOptions#collisions() collisions
 * policy} and is never a refusal: {@code keep} counts it as a version conflict, and any other policy fails the run, even
 * when {@code keep_going} is set.
 */
public final class IntoPageWriter {

    private static final int MAX_FAILURE_DETAIL_LENGTH = 1024;
    /**
     * Tells the user who hits a collision that the option exists, as they would not otherwise find out. Replace is qualified because
     * the analyzer rejects it on a data stream, which also has collisions.
     */
    private static final String COLLISIONS_HINT = "; to handle documents that already exist, use \""
        + IntoOptions.Option.COLLISIONS.optionName()
        + "\": \""
        + IntoOptions.Collisions.KEEP.optionValue()
        + "\", or \""
        + IntoOptions.Option.COLLISIONS.optionName()
        + "\": \""
        + IntoOptions.Collisions.REPLACE.optionValue()
        + "\" (not supported on a data stream)";
    private static final String EMPTY_ID_REASON = "if _id is specified it must not be empty";
    private static final String MULTI_VALUED_ID_MESSAGE = "INTO [{}] failed: [{}] must be single-valued, found [{}]";

    /** Holds the query-scoped configuration, counters, and failure state across drivers for one {@code INTO} run. */
    public static final class Factory {
        public enum Indexing {
            /** Sends incoming pages to the destination as ESCF batches. */
            INDEX,
            /** Releases incoming pages without writing anything. */
            DISCARD
        }

        private final String destination;
        private final List<String> columns;
        private final List<DataType> types;
        private final IntoOptions options;
        @Nullable
        private final EscfBulkWriter writer;
        /** Position of the {@code _id} column in {@link #columns}, or {@code -1} when the destination assigns every id. */
        private final int idColumnPosition;
        private final DocWriteRequest.OpType opType;
        private final IntoOptions.Collisions collisions;
        private final AtomicLong total = new AtomicLong();
        private final AtomicLong created = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();
        // The reasons for the first refusals. Every access synchronizes on this list, because bulks are acknowledged on several threads.
        private final List<String> failureDetails = new ArrayList<>(IntoStats.MAX_FAILURE_DETAILS);
        private final AtomicLong updated = new AtomicLong();
        private final AtomicLong versionConflicts = new AtomicLong();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final AtomicInteger openProducers = new AtomicInteger();
        private final List<IntoPageWriter> drivers = new CopyOnWriteArrayList<>();
        private volatile boolean settled;

        public static Factory createFactory(
            EscfBulkService bulks,
            Client client,
            CircuitBreaker breaker,
            String destination,
            List<Attribute> output,
            Indexing indexing,
            IntoOptions options
        ) {
            return new Factory(
                bulks,
                client,
                breaker,
                destination,
                Expressions.names(output),
                output.stream().map(Attribute::dataType).toList(),
                indexing,
                options
            );
        }

        private Factory(
            EscfBulkService bulks,
            Client client,
            CircuitBreaker breaker,
            String destination,
            List<String> columns,
            List<DataType> types,
            Indexing indexing,
            IntoOptions options
        ) {
            this.destination = destination;
            this.columns = columns;
            this.types = types;
            this.options = options;
            this.idColumnPosition = columns.indexOf(IdFieldMapper.NAME);
            this.collisions = options.collisions();
            this.opType = switch (collisions) {
                case ABORT, KEEP -> DocWriteRequest.OpType.CREATE;
                case REPLACE -> DocWriteRequest.OpType.INDEX;
            };
            this.writer = indexing == Indexing.INDEX
                ? bulks.newWriter(client, breaker, destination, bulks.defaultPolicy(), this::onResponse, this::onCapacity)
                : null;
        }

        String destination() {
            return destination;
        }

        public IntoPageWriter create(DriverContext driverContext) {
            openProducers.incrementAndGet();
            IntoPageWriter pageWriter = new IntoPageWriter(
                this,
                writer == null ? null : writer.newProducer(),
                Objects.requireNonNull(driverContext),
                drivers.isEmpty()
            );
            drivers.add(pageWriter);
            return pageWriter;
        }

        private Exception onResponse(BulkResponse response) {
            long createdItems = 0;
            long updatedItems = 0;
            long conflictingItems = 0;
            long refusedItems = 0;
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    BulkItemResponse.Failure refused = item.getFailure();
                    if (refused.getStatus() == RestStatus.CONFLICT && collisions == IntoOptions.Collisions.KEEP) {
                        conflictingItems++;
                        continue;
                    }
                    if (options.keepGoing() && isRefusal(refused)) {
                        recordRefusal(refused);
                        refusedItems++;
                        continue;
                    }
                    Exception cause = refused.getCause();
                    boolean collidedUnderAbort = refused.getStatus() == RestStatus.CONFLICT && collisions == IntoOptions.Collisions.ABORT;
                    ElasticsearchStatusException failed = new ElasticsearchStatusException(
                        "INTO [{}] failed: {}{}",
                        refused.getStatus(),
                        cause,
                        destination,
                        cause.getMessage(),
                        collidedUnderAbort ? COLLISIONS_HINT : ""
                    );
                    failure.compareAndSet(null, failed);
                    return failed;
                }
                DocWriteResponse.Result result = item.getResponse().getResult();
                switch (result) {
                    case CREATED -> createdItems++;
                    case UPDATED -> updatedItems++;
                    case DELETED, NOT_FOUND, NOOP -> throw new IllegalStateException("unexpected bulk item result [" + result + "]");
                }
            }
            total.addAndGet(response.getItems().length);
            created.addAndGet(createdItems);
            updated.addAndGet(updatedItems);
            versionConflicts.addAndGet(conflictingItems);
            failures.addAndGet(refusedItems);
            return null;
        }

        /**
         * A row is refused when the destination does not accept the row itself: a 400, such as a field that its mapping rejects, or a
         * {@link MapperException}, which has no status of its own and so is reported as a 500. Any other status is about the caller,
         * the destination or the cluster, so it fails the query instead of being counted as a refusal.
         */
        private static boolean isRefusal(BulkItemResponse.Failure failure) {
            // The mapping update can be handled by another node, which wraps the rejection in a transport exception
            return failure.getStatus() == RestStatus.BAD_REQUEST
                || ExceptionsHelper.unwrapCause(failure.getCause()) instanceof MapperException;
        }

        private void recordRefusal(BulkItemResponse.Failure refusal) {
            // Without the transport exception around it, which names a node that the caller has no business with
            Throwable cause = ExceptionsHelper.unwrapCause(refusal.getCause());
            String reason = Objects.requireNonNullElse(cause.getMessage(), cause.getClass().getSimpleName());
            recordFailureDetail(refusal.getStatus(), reason);
        }

        /**
         * Counts rows that were refused before they were sent, because the destination would have refused the whole bulk that carried
         * them, not only them.
         */
        private void recordUnsentRefusals(long refused, List<String> reasons) {
            total.addAndGet(refused);
            failures.addAndGet(refused);
            for (String reason : reasons) {
                recordFailureDetail(RestStatus.BAD_REQUEST, reason);
            }
        }

        private void recordFailureDetail(RestStatus status, String reason) {
            synchronized (failureDetails) {
                if (failureDetails.size() < IntoStats.MAX_FAILURE_DETAILS) {
                    String detail = "[" + status.getStatus() + "] " + reason;
                    failureDetails.add(Strings.cleanTruncate(detail, MAX_FAILURE_DETAIL_LENGTH));
                }
            }
        }

        private Exception recordWriterFailure(Exception failed) {
            // A missing destination is wrapped so DataNodeRequestSender does not take it for a missing source shard and retry that
            boolean destinationUnavailable = TransportActions.isShardNotAvailableException(failed);
            failure.compareAndSet(
                null,
                destinationUnavailable
                    ? new ElasticsearchStatusException(
                        "INTO [{}] failed: {}",
                        ExceptionsHelper.status(failed),
                        failed,
                        destination,
                        failed.getMessage()
                    )
                    : failed
            );
            return failure.get();
        }

        private void onCapacity() {
            for (IntoPageWriter pageWriter : drivers) {
                pageWriter.wake();
            }
        }

        private void producerClosed() {
            if (openProducers.decrementAndGet() == 0) {
                writer.finish(ActionListener.wrap(ignored -> settled(null), this::settled));
            }
        }

        private void settled(Exception failed) {
            if (failed != null) {
                recordWriterFailure(failed);
            }
            settled = true;
            for (IntoPageWriter pageWriter : drivers) {
                pageWriter.settled();
            }
        }

        IntoStats stats() {
            synchronized (failureDetails) {
                return IntoStats.builder()
                    .total(total.get())
                    .created(created.get())
                    .updated(updated.get())
                    .versionConflicts(versionConflicts.get())
                    .failures(failures.get())
                    .failureDetails(failureDetails)
                    .build();
            }
        }

        /** Returns the first bulk or item failure recorded by this run, or {@code null} if every write succeeded. */
        @Nullable
        public Exception failure() {
            return failure.get();
        }

        /**
         * Fails the query when {@code target} cannot be written as a pre-built ESCF batch. Checked on the coordinator before the
         * query is dispatched, and again where a node plans the write, since the destination can change in between.
         */
        public static void requireBatchDestination(boolean batchIndexingSupported, ProjectMetadata project, String target) {
            // TODO: authorize write access to the destination before reporting destination-specific errors below, which would
            // otherwise reveal an inaccessible index's pipeline or routing. Tracked with the destination authorization in EsqlSession.
            if (batchIndexingSupported == false) {
                throw rejection("INTO [%s] requires batch indexing", target);
            }
            if (hasPipeline(project, target)) {
                throw rejection("INTO [%s] does not support ingest pipelines", target);
            }
            if (routesFromSource(project, target)) {
                throw rejection("INTO [%s] does not support indices that route from _source", target);
            }
        }

        private static boolean hasPipeline(ProjectMetadata project, String target) {
            IndexRequest probe = new IndexRequest(target);
            IngestService.setPipelineOnRequest(probe, IngestService.resolvePipelines(probe, probe, project, System.currentTimeMillis()));
            return IngestService.hasPipeline(probe);
        }

        private static ElasticsearchStatusException rejection(String format, Object... args) {
            return new ElasticsearchStatusException(Strings.format(format, args), RestStatus.BAD_REQUEST);
        }

        private static boolean routesFromSource(ProjectMetadata project, String target) {
            IndexAbstraction abstraction = project.getIndicesLookup().get(target);
            if (abstraction == null) {
                // Any matching template can create the index with source routing. Applying the template
                // is what resolves that routing, so a match is rejected.
                return MetadataIndexTemplateService.findV2Template(project, target, false) != null
                    || MetadataIndexTemplateService.findV1Templates(project, target, Boolean.FALSE).isEmpty() == false;
            }
            Index writeIndex = abstraction.getWriteIndex();
            return writeIndex != null
                && IndexRouting.fromIndexMetadata(project.getIndexSafe(writeIndex)) instanceof IndexRouting.ExtractFromSource;
        }
    }

    private final Factory sink;
    @Nullable
    private final EscfBulkWriter.Producer producer;
    private final DriverContext driverContext;
    // The factory's counters are query-wide, so only one driver reports them; the merge step sums every driver's row.
    private final boolean reportsStats;

    private boolean finished;
    private boolean producerClosed;
    private final AtomicBoolean asyncAction = new AtomicBoolean();
    // null whenever no driver is blocked on this writer; allocated in isBlocked() and cleared in wake().
    @Nullable
    private SubscribableListener<Void> waiting;

    private IntoPageWriter(Factory sink, @Nullable EscfBulkWriter.Producer producer, DriverContext driverContext, boolean reportsStats) {
        this.sink = sink;
        this.producer = producer;
        this.driverContext = driverContext;
        this.reportsStats = reportsStats;
        if (producer == null) {
            // Nothing is written, so there is nothing to settle.
            producerClosed = true;
        }
    }

    IntoStats stats() {
        return reportsStats ? sink.stats() : IntoStats.EMPTY;
    }

    /** Returns the first bulk or item failure recorded by this run, or {@code null} if every write succeeded. */
    @Nullable
    Exception failure() {
        return sink.failure();
    }

    public void consume(Page page) {
        try {
            if (producer != null && page.getPositionCount() > 0 && sink.failure.get() == null) {
                if (sink.idColumnPosition >= 0) {
                    requireSingleValuedIds(page);
                }
                int[] rows = rowsWithValidIds(page);
                if (rows == null || rows.length > 0) {
                    if (asyncAction.compareAndSet(false, true)) {
                        driverContext.addAsyncAction();
                    }
                    producer.add(
                        new PageRows(page, sink.columns, sink.types, sink.destination, sink.idColumnPosition, sink.opType, rows),
                        false
                    );
                    Exception failed = sink.writer.failure();
                    if (failed != null) {
                        throw ExceptionsHelper.convertToRuntime(sink.recordWriterFailure(failed));
                    }
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Rejects the page if the {@code _id} of any row has several values, and fails every driver of the run, not just this one, so that
     * a row that can never be written does not keep the others sending. Does nothing otherwise.
     */
    private void requireSingleValuedIds(Page page) {
        BytesRefBlock ids = page.getBlock(sink.idColumnPosition);
        for (int row = 0; row < page.getPositionCount(); row++) {
            int count = ids.getValueCount(row);
            if (count > 1) {
                BytesRef scratch = new BytesRef();
                int start = ids.getFirstValueIndex(row);
                List<String> values = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    values.add(ids.getBytesRef(start + i, scratch).utf8ToString());
                }
                var failed = new ElasticsearchStatusException(
                    MULTI_VALUED_ID_MESSAGE,
                    RestStatus.BAD_REQUEST,
                    sink.destination,
                    IdFieldMapper.NAME,
                    values
                );
                sink.failure.compareAndSet(null, failed);
                throw failed;
            }
        }
    }

    /**
     * The positions of the rows of {@code page} whose {@code _id} the destination can take, or {@code null} when every row has one.
     * The bulk fails as a whole, or reports the same failure against all its rows, for an {@code _id} that is empty or too long, so
     * the valid rows next to it would be lost with it. A row like that fails the run, as soon as it is found, or, under
     * {@code keep_going}, is counted as refused and left out, as it would be if the destination had refused it alone.
     */
    @Nullable
    private int[] rowsWithValidIds(Page page) {
        if (sink.idColumnPosition < 0) {
            return null;
        }
        BytesRefBlock ids = page.getBlock(sink.idColumnPosition);
        int[] valid = new int[page.getPositionCount()];
        int validCount = 0;
        long refused = 0;
        // Only the details that the stats can hold, as every refusal is counted but only a sample of them is reported
        List<String> reasons = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (int row = 0; row < page.getPositionCount(); row++) {
            int length = ids.isNull(row) ? -1 : ids.getBytesRef(ids.getFirstValueIndex(row), scratch).length;
            if (length != 0 && length <= IndexRequest.MAX_DOCUMENT_ID_LENGTH_IN_BYTES) {
                valid[validCount++] = row;
                continue;
            }
            if (sink.options.keepGoing() == false) {
                var failed = new ElasticsearchStatusException(
                    "INTO [{}] failed: {}",
                    RestStatus.BAD_REQUEST,
                    sink.destination,
                    invalidIdReason(length)
                );
                sink.failure.compareAndSet(null, failed);
                throw failed;
            }
            refused++;
            if (reasons.size() < IntoStats.MAX_FAILURE_DETAILS) {
                reasons.add(invalidIdReason(length));
            }
        }
        if (refused == 0) {
            return null;
        }
        sink.recordUnsentRefusals(refused, reasons);
        return Arrays.copyOf(valid, validCount);
    }

    private static String invalidIdReason(int length) {
        return length == 0
            ? EMPTY_ID_REASON
            : "_id is too long, must be no longer than " + IndexRequest.MAX_DOCUMENT_ID_LENGTH_IN_BYTES + " bytes but was: " + length;
    }

    public void finish() {
        closeProducer();
        wake(() -> finished = true);
    }

    public void close() {
        // finish() keeps the async action until the bulks settle. A failure never calls finish().
        if (producerClosed == false) {
            // Closing without finish() means the driver failed or was cancelled, so drop the rows not sent yet instead of
            // letting the producer hand its open batch to the writer to be flushed.
            sink.writer.close();
            closeProducer();
            releaseAsyncAction();
        }
    }

    private void closeProducer() {
        synchronized (this) {
            if (producerClosed) {
                return;
            }
            producerClosed = true;
        }
        producer.close();
        sink.producerClosed();
    }

    private void settled() {
        releaseAsyncAction();
        wake(() -> {});
    }

    private void releaseAsyncAction() {
        if (asyncAction.compareAndSet(true, false)) {
            driverContext.removeAsyncAction();
        }
    }

    private void wake() {
        wake(() -> {});
    }

    private void wake(Runnable change) {
        SubscribableListener<Void> release = null;
        synchronized (this) {
            change.run();
            if (mayContinue()) {
                release = waiting;
                waiting = null;
            }
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    public synchronized IsBlockedResult isBlocked() {
        if (mayContinue()) {
            return Operator.NOT_BLOCKED;
        }
        if (waiting == null) {
            waiting = new SubscribableListener<>();
        }
        return new IsBlockedResult(waiting, "INTO " + sink.destination);
    }

    private boolean mayContinue() {
        if (Thread.holdsLock(this) == false) {
            throw new IllegalStateException("mayContinue() must be called while holding this monitor");
        }
        if (producer == null) {
            return true;
        }
        return finished ? sink.settled : producer.hasCapacity();
    }

    public synchronized boolean isFinished() {
        return finished && (producer == null || sink.settled);
    }

    private record PageRows(
        Page page,
        List<String> columns,
        List<DataType> types,
        String destination,
        int idColumnPosition,
        DocWriteRequest.OpType opType,
        @Nullable int[] rows
    ) implements EscfBulkWriter.Rows {
        /** {@code rows} are the positions of the rows of {@code page} to write, or {@code null} for all of them. */
        @Override
        public int count() {
            return rows == null ? page.getPositionCount() : rows.length;
        }

        @Override
        public void append(EscfBatchBuilder batch) {
            PageToEscfBatch.append(batch, page, columns, types, idColumnPosition, rows);
        }

        @Override
        public IndexRequest request(int row) {
            return new IndexRequest(destination).id(documentId(rows == null ? row : rows[row])).opType(opType);
        }

        /** The {@code _id} value of {@code row}, or {@code null} when the destination assigns the id. */
        @Nullable
        private String documentId(int row) {
            if (idColumnPosition < 0) {
                return null;
            }
            Block ids = page.getBlock(idColumnPosition);
            if (ids.isNull(row)) {
                return null;
            }
            BytesRefBlock block = (BytesRefBlock) ids;
            return block.getBytesRef(block.getFirstValueIndex(row), new BytesRef()).utf8ToString();
        }
    }
}
