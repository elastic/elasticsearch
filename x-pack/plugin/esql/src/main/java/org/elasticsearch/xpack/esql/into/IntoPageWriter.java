/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Writes the rows an {@code INTO} query produces, one bulk per page as the pages arrive, so only the pages in flight
 * are held however large the result is.
 * <p>
 * A row becomes a document whose fields are the query's columns. A column named {@value #ID_COLUMN} is the document's
 * id rather than a field; without one the destination generates ids. What happens when an id already exists is
 * governed by {@link IntoOptions#collisions()}; any other refusal fails the query unless {@link IntoOptions#keepGoing()},
 * in which case it is counted and the rest is written. A bulk that fails as a whole (the destination refused the
 * request, or the caller may not write to it) fails the query either way, since none of it was written.
 * <p>
 * A data stream only takes {@code create} ops by name; {@link IntoDestination} refuses {@code collisions=replace} for
 * one, so that is the only op type a data stream ever sees here.
 * <p>
 * Takes part in the driver's lifecycle: the driver is held back while {@link #MAX_CONCURRENT_BULKS} bulks are
 * outstanding, and the query only completes once every bulk has been acknowledged and, unless {@code refresh=false}, the
 * destination has been refreshed; a refresh that fails is reported as a warning rather than failing the query, since
 * the rows are written. Each outstanding request is also registered with the {@link DriverContext}, so a
 * cancelled query waits for its writes to settle before it reports. An async {@code STOP} reaches the writer through
 * the driver's stop hooks: from then on no further page is written, the bulks in flight settle, and the query
 * answers with the statistics of what was written, flagged as partial.
 */
public final class IntoPageWriter implements PageConsumer {

    /** The output column, if present, that becomes the document id. */
    public static final String ID_COLUMN = "_id";

    /** How many bulks may be outstanding before the driver is asked to wait. */
    static final int MAX_CONCURRENT_BULKS = 3;

    private final Client client;
    /** The context the query arrived in, so the writes are made with the caller's privileges. */
    private final Supplier<ThreadContext.StoredContext> caller;
    private final String destination;
    private final IntoOptions options;
    private final String[] columns;
    private final ResponseValueUtils.BlockValueExtractor[] extractors;
    private final int idColumn;
    private final DocWriteRequest.OpType opType;

    private final IntoStats stats = new IntoStats();
    /** The failure that fails the query, if any. Only the first is kept. */
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    private DriverContext driverContext;
    private int inFlight;
    private boolean finished;
    private boolean stopped;
    private boolean refreshing;
    @Nullable
    private String refreshWarning;
    private boolean done;
    private SubscribableListener<Void> waiting;

    public IntoPageWriter(Client client, String destination, IntoOptions options, List<Attribute> output) {
        this.client = client;
        this.caller = client.threadPool().getThreadContext().newRestorableContext(false);
        this.destination = destination;
        this.options = options;
        this.columns = new String[output.size()];
        this.extractors = new ResponseValueUtils.BlockValueExtractor[output.size()];
        int id = -1;
        for (int i = 0; i < output.size(); i++) {
            Attribute attribute = output.get(i);
            columns[i] = attribute.name();
            // Dates are rendered in UTC whatever the query's time zone, so the destination parses them unambiguously.
            extractors[i] = ResponseValueUtils.valueExtractorFor(attribute.dataType(), ZoneOffset.UTC);
            if (ID_COLUMN.equals(attribute.name())) {
                id = i;
            }
        }
        this.idColumn = id;
        this.opType = options.collisions() == IntoOptions.Collisions.REPLACE ? DocWriteRequest.OpType.INDEX : DocWriteRequest.OpType.CREATE;
    }

    /** What was written. Only meaningful once {@link #isFinished()}. */
    public IntoStats stats() {
        return stats;
    }

    /** The failure that fails the query, or null when it may report its {@link #stats()}. */
    @Nullable
    public Exception failure() {
        return failure.get();
    }

    @Override
    public synchronized void bind(DriverContext driverContext) {
        if (this.driverContext != null && this.driverContext != driverContext) {
            throw new IllegalStateException("INTO writes from a single driver");
        }
        this.driverContext = driverContext;
        driverContext.addStopHook(this::stop);
    }

    /**
     * Async {@code STOP}: pages offered from now on are dropped rather than written, so the destination stops growing
     * as soon as the stop is asked for rather than when the reads wind down. A bulk already sent cannot be taken back,
     * so the bulks in flight settle and are counted. Returns whether this cut a write that was still going, which is
     * what marks the response partial.
     */
    private synchronized boolean stop() {
        if (stopped || finished) {
            return false;
        }
        stopped = true;
        return true;
    }

    private synchronized boolean stopped() {
        return stopped;
    }

    @Override
    public void accept(Page page) {
        try {
            if (failure.get() != null || stopped()) {
                // Dropped rather than written, so the driver can finish: the failure is reported once, and a stopped
                // write does not grow the destination any further.
                return;
            }
            final BulkRequest bulk = bulkFor(page);
            if (bulk.numberOfActions() > 0) {
                send(bulk);
            }
        } finally {
            // The rows are in the bulk now.
            page.releaseBlocks();
        }
    }

    /** Turns one page into one bulk: a row becomes a document whose fields are the query's columns. */
    private BulkRequest bulkFor(Page page) {
        final BulkRequest bulk = new BulkRequest();
        final BytesRef scratch = new BytesRef();
        final List<String> refused = new ArrayList<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            String id = null;
            if (idColumn >= 0) {
                Block idBlock = page.getBlock(idColumn);
                if (idBlock.isNull(position) == false) {
                    if (idBlock.getValueCount(position) != 1) {
                        refused.add(destination + "/: row has a multi-valued [" + ID_COLUMN + "]");
                        continue;
                    }
                    id = (String) extractors[idColumn].extract(idBlock, idBlock.getFirstValueIndex(position), scratch);
                }
            }
            final Map<String, Object> source = new HashMap<>();
            for (int column = 0; column < columns.length; column++) {
                if (column == idColumn) {
                    continue;
                }
                Object value = valueAt(page.getBlock(column), extractors[column], position, scratch);
                if (value != null) {
                    source.put(columns[column], value);
                }
            }
            final IndexRequest request = new IndexRequest(destination).opType(opType).source(source);
            if (id != null) {
                request.id(id);
            }
            bulk.add(request);
        }
        synchronized (this) {
            stats.countRows(page.getPositionCount());
            refused.forEach(message -> refused(message, RestStatus.BAD_REQUEST, null));
        }
        return bulk;
    }

    /** The value at a position: null, a single value, or a list for a multi-valued field. */
    private static Object valueAt(Block block, ResponseValueUtils.BlockValueExtractor extractor, int position, BytesRef scratch) {
        if (block.isNull(position)) {
            return null;
        }
        int count = block.getValueCount(position);
        int start = block.getFirstValueIndex(position);
        if (count == 1) {
            return extractor.extract(block, start, scratch);
        }
        List<Object> values = new ArrayList<>(count);
        for (int i = start; i < start + count; i++) {
            values.add(extractor.extract(block, i, scratch));
        }
        return values;
    }

    private void send(BulkRequest bulk) {
        synchronized (this) {
            inFlight++;
            if (driverContext != null) {
                driverContext.addAsyncAction();
            }
        }
        // A driver thread is not the thread the query arrived on, so the caller's context is restored around the
        // write: the destination is authorized against whoever asked for it.
        try (ThreadContext.StoredContext ignored = caller.get()) {
            client.execute(TransportBulkAction.TYPE, bulk, ActionListener.wrap(this::onBulkResponse, this::onBulkFailure));
        }
    }

    private void onBulkResponse(BulkResponse response) {
        synchronized (this) {
            stats.countBatch();
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    onItemFailure(item);
                } else if (item.getResponse().getResult() == DocWriteResponse.Result.CREATED) {
                    stats.countCreated();
                } else {
                    stats.countUpdated();
                }
            }
        }
        settled();
    }

    private void onItemFailure(BulkItemResponse item) {
        assert Thread.holdsLock(this);
        BulkItemResponse.Failure itemFailure = item.getFailure();
        if (itemFailure.getStatus() == RestStatus.UNAUTHORIZED || itemFailure.getStatus() == RestStatus.FORBIDDEN) {
            // The caller may not write to the destination: that is not a row the destination dislikes, so keep_going
            // does not apply and the query fails, as it does when the whole bulk is refused.
            recordFailure(
                new ElasticsearchStatusException(
                    "INTO [{}] failed: {}",
                    itemFailure.getStatus(),
                    itemFailure.getCause(),
                    destination,
                    itemFailure.getCause().getMessage()
                )
            );
        } else if (itemFailure.getStatus() == RestStatus.CONFLICT) {
            switch (options.collisions()) {
                case KEEP -> stats.countVersionConflict();
                case ABORT -> recordFailure(
                    new ElasticsearchStatusException(
                        "INTO [{}] failed: document [{}] already exists; use \"collisions\": \"replace\" or \"collisions\": \"keep\"",
                        RestStatus.CONFLICT,
                        itemFailure.getCause(),
                        destination,
                        item.getId()
                    )
                );
                // Replacing never collides, so a conflict is a refusal like any other.
                case REPLACE -> refused(describe(item), itemFailure.getStatus(), itemFailure.getCause());
            }
        } else {
            refused(describe(item), itemFailure.getStatus(), itemFailure.getCause());
        }
    }

    /** A row the destination would not take: counted with {@code keep_going}, otherwise the query fails on it. */
    private void refused(String message, RestStatus status, @Nullable Throwable cause) {
        assert Thread.holdsLock(this);
        if (options.keepGoing()) {
            stats.countFailed(message);
        } else {
            recordFailure(
                new ElasticsearchStatusException(
                    "INTO [{}] failed: {}; use \"" + IntoOptions.KEEP_GOING + "\": true to skip rows the destination refuses",
                    status,
                    cause,
                    destination,
                    message
                )
            );
        }
    }

    /** Which document was refused and why, without the exception's class name in front of the reason. */
    private static String describe(BulkItemResponse item) {
        return item.getIndex() + "/" + item.getId() + ": " + item.getFailure().getCause().getMessage();
    }

    /** The whole bulk failed, so nothing in it was written and the query cannot be said to have succeeded. */
    private void onBulkFailure(Exception e) {
        recordFailure(e);
        settled();
    }

    /** Keeps only the first failure. */
    private void recordFailure(Exception e) {
        failure.compareAndSet(null, e);
    }

    /** One request has come back. Releases the driver if it was waiting on this, and finishes up if it was the last. */
    private void settled() {
        final boolean refresh;
        final SubscribableListener<Void> release;
        synchronized (this) {
            inFlight--;
            if (driverContext != null) {
                driverContext.removeAsyncAction();
            }
            refresh = finishIfSettled();
            release = mayContinue() ? takeWaiting() : null;
        }
        if (refresh) {
            refresh();
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    @Override
    public void finish() {
        final boolean refresh;
        final SubscribableListener<Void> release;
        synchronized (this) {
            finished = true;
            refresh = finishIfSettled();
            release = mayContinue() ? takeWaiting() : null;
        }
        if (refresh) {
            refresh();
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    /**
     * Once the last page has been handed over and every bulk has come back, either the writer is done or, when a
     * refresh was asked for and nothing failed, it is started. Returns whether the caller must start the refresh,
     * which is done outside the lock.
     */
    private boolean finishIfSettled() {
        assert Thread.holdsLock(this);
        if (finished == false || inFlight > 0 || refreshing || done) {
            return false;
        }
        if (options.refresh() && failure.get() == null) {
            refreshing = true;
            if (driverContext != null) {
                driverContext.addAsyncAction();
            }
            return true;
        }
        done = true;
        return false;
    }

    /** Why the final refresh did not happen, or null if it did or was not asked for. It does not fail the query. */
    @Nullable
    public synchronized String refreshWarning() {
        return refreshWarning;
    }

    private void refresh() {
        final ActionListener<BroadcastResponse> listener = ActionListener.wrap(response -> {
            if (response.getFailedShards() > 0) {
                refreshed(
                    new ElasticsearchException(
                        "refresh failed on [{}] shards",
                        response.getShardFailures()[0].getCause(),
                        response.getFailedShards()
                    )
                );
            } else {
                refreshed(null);
            }
        }, this::refreshed);
        try (ThreadContext.StoredContext ignored = caller.get()) {
            client.admin().indices().refresh(new RefreshRequest(destination), listener);
        }
    }

    private void refreshed(@Nullable Exception e) {
        final SubscribableListener<Void> release;
        synchronized (this) {
            if (e != null) {
                // The rows are in, so the query succeeded; what is lost is only when they become searchable. Failing here
                // would invite a retry that writes them again, so it is reported instead.
                refreshWarning = Strings.format(
                    "INTO [%s] wrote its rows but could not refresh the destination (%s), so they may not be searchable yet",
                    destination,
                    e.getMessage()
                );
            }
            refreshing = false;
            done = true;
            if (driverContext != null) {
                driverContext.removeAsyncAction();
            }
            release = takeWaiting();
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    @Override
    public synchronized IsBlockedResult isBlocked() {
        if (mayContinue()) {
            return Operator.NOT_BLOCKED;
        }
        if (waiting == null) {
            waiting = new SubscribableListener<>();
        }
        return new IsBlockedResult(waiting, "INTO " + destination);
    }

    /**
     * Whether the driver may carry on: not while {@link #MAX_CONCURRENT_BULKS} are outstanding, and once the last page
     * has been handed over, not until every bulk has come back and any refresh has completed. Returning a listener
     * whenever this is false matters: a parked driver with nothing blocking it would spin.
     */
    private boolean mayContinue() {
        assert Thread.holdsLock(this);
        return finished ? done : inFlight < MAX_CONCURRENT_BULKS;
    }

    private SubscribableListener<Void> takeWaiting() {
        assert Thread.holdsLock(this);
        final SubscribableListener<Void> release = waiting;
        waiting = null;
        return release;
    }

    @Override
    public synchronized boolean isFinished() {
        return done;
    }

    @Override
    public void close() {
        // Nothing held here; outstanding requests are registered with the driver, which waits for them.
    }
}
