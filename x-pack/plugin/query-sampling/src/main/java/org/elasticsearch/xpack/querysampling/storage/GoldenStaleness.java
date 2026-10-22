/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * Tells which golden queries have an out-of-date ground truth by comparing the current {@link DataState} with the stored one,
 * which is far cheaper than recomputing. The probes run one after the other, as the caller, so they only see what the caller may.
 */
public final class GoldenStaleness {

    private static final Logger logger = LogManager.getLogger(GoldenStaleness.class);

    public record Result(long version, int checked, int fresh, int stale, int unknown, int failed) {}

    private final GoldenReader reader;
    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> probe;
    private final Executor executor;

    public GoldenStaleness(GoldenReader reader, BiConsumer<SearchRequest, ActionListener<SearchResponse>> probe, Executor executor) {
        this.reader = reader;
        this.probe = probe;
        this.executor = executor;
    }

    /** Checks up to {@code max} queries of {@code version}, or of the latest completed one if it is 0. */
    public void check(long version, int max, ActionListener<Result> listener) {
        if (version > 0) {
            read(version, max, listener);
            return;
        }
        reader.latestCompleted(ActionListener.wrap(latest -> {
            if (latest == 0) {
                listener.onResponse(new Result(0, 0, 0, 0, 0, 0));
            } else {
                read(latest, max, listener);
            }
        }, listener::onFailure));
    }

    private void read(long version, int max, ActionListener<Result> listener) {
        reader.read(version, max, ActionListener.wrap(read -> classify(read.samples(), ActionListener.wrap(statuses -> {
            int[] counts = new int[Status.values().length];
            statuses.forEach(status -> counts[status.ordinal()]++);
            int failed = read.unreadable() + counts[Status.FAILED.ordinal()];
            if (failed > 0) {
                logger.warn(
                    "[{}] of [{}] golden queries of version [{}] could not be checked",
                    failed,
                    read.samples().size() + read.unreadable(),
                    version
                );
            }
            listener.onResponse(
                new Result(
                    version,
                    read.samples().size() + read.unreadable(),
                    counts[Status.FRESH.ordinal()],
                    counts[Status.STALE.ordinal()],
                    counts[Status.UNKNOWN.ordinal()],
                    read.unreadable() + counts[Status.FAILED.ordinal()]
                )
            );
        }, listener::onFailure)), listener::onFailure));
    }

    public enum Status {
        FRESH,
        STALE,
        /** Stored without a data state, so it cannot be told. */
        UNKNOWN,
        FAILED
    }

    private static DataState stateOf(StoredSample record) {
        return record.groundTruth() == null ? null : record.groundTruth().dataState();
    }

    /** Responds with the status of each query, in order. */
    public void classify(List<StoredSample> records, ActionListener<List<Status>> listener) {
        next(records, new ArrayList<>(records.size()), listener);
    }

    // the probe before may have been completed by a network thread, which must not start the next one
    private void resume(List<StoredSample> records, List<Status> statuses, ActionListener<List<Status>> listener) {
        try {
            executor.execute(ActionRunnable.wrap(listener, l -> next(records, statuses, l)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void next(List<StoredSample> records, List<Status> statuses, ActionListener<List<Status>> listener) {
        // the queries that cannot be told are taken in a loop, as no search comes between them to end the call stack
        while (statuses.size() < records.size() && stateOf(records.get(statuses.size())) == null) {
            statuses.add(Status.UNKNOWN);
        }
        int index = statuses.size();
        if (index == records.size()) {
            listener.onResponse(statuses);
            return;
        }
        StoredSample record = records.get(index);
        DataState stored = stateOf(record);
        // a search function may complete the listener and then still throw, only the first outcome counts
        ActionListener<DataState> done = ActionListener.notifyOnce(ActionListener.wrap(now -> {
            statuses.add(stored.equals(now) ? Status.FRESH : Status.STALE);
            resume(records, statuses, listener);
        }, e -> {
            // there can be thousands of these, so each is at debug, and the number is in the summary
            logger.debug("failed to look at the state of the data of a golden query", e);
            statuses.add(Status.FAILED);
            resume(records, statuses, listener);
        }));
        try {
            DataState.read(probe, record.search().query(), done);
        } catch (Exception e) {
            done.onFailure(e);
        }
    }
}
