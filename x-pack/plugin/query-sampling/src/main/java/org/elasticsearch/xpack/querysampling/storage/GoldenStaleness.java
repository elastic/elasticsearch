/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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

    public GoldenStaleness(GoldenReader reader, BiConsumer<SearchRequest, ActionListener<SearchResponse>> probe) {
        this.reader = reader;
        this.probe = probe;
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
        ActionListener<DataState> done = new ActionListener<>() {
            // a search function may complete the listener and then still throw, only the first outcome counts
            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void onResponse(DataState now) {
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                statuses.add(stored.equals(now) ? Status.FRESH : Status.STALE);
                next(records, statuses, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                logger.debug("failed to look at the state of the data of a golden query", e);
                statuses.add(Status.FAILED);
                next(records, statuses, listener);
            }
        };
        try {
            DataState.read(probe, record.search().query(), done);
        } catch (Exception e) {
            done.onFailure(e);
        }
    }
}
