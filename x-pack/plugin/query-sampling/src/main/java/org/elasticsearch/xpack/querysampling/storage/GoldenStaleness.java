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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Tells which queries of a version of the golden dataset have a ground truth that is out of date: those whose data is not the
 * same any more as when it was computed. Each query is looked at by one search that only counts what its exact search would
 * have scanned and sums their sequence numbers, and compares that with what was stored with the ground truth, so what it costs
 * is far less than computing the ground truth again.
 * <p>
 * The searches are done one after the other, as whoever asked, so that they only see what that person may see.
 */
public final class GoldenStaleness {

    private static final Logger logger = LogManager.getLogger(GoldenStaleness.class);

    /**
     * @param version   the version that was looked at, 0 if there is none to look at
     * @param checked   queries of the version that were looked at
     * @param fresh     of those, queries whose data is as it was
     * @param stale     queries whose data changed, so that their ground truth cannot be trusted
     * @param unknown   queries that were stored without the state of the data, which cannot be told
     * @param failed    queries that could not be read, and queries whose data could not be looked at
     */
    public record Result(long version, int checked, int fresh, int stale, int unknown, int failed) {}

    private final GoldenReader reader;
    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> probe;

    /**
     * @param reader reads the golden dataset, as the plugin
     * @param probe  runs the searches that tell the state of the data, as the caller
     */
    public GoldenStaleness(GoldenReader reader, BiConsumer<SearchRequest, ActionListener<SearchResponse>> probe) {
        this.reader = reader;
        this.probe = probe;
    }

    /**
     * Looks at up to {@code max} queries of a version.
     *
     * @param version the version, or 0 for the latest that is complete
     */
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
        reader.read(
            version,
            max,
            ActionListener.wrap(read -> next(version, read.records(), read.unreadable(), 0, new int[4], listener), listener::onFailure)
        );
    }

    /**
     * Looks at the query at {@code index}, and then at the next.
     *
     * @param counts how many were found fresh, stale and unknown so far, and how many could not be looked at
     */
    private void next(long version, List<StoredSample> records, int unreadable, int index, int[] counts, ActionListener<Result> listener) {
        if (index == records.size()) {
            listener.onResponse(new Result(version, records.size() + unreadable, counts[0], counts[1], counts[2], unreadable + counts[3]));
            return;
        }
        StoredSample record = records.get(index);
        DataState stored = record.groundTruth() == null ? null : record.groundTruth().dataState();
        if (stored == null) {
            counts[2]++;
            next(version, records, unreadable, index + 1, counts, listener);
            return;
        }
        ActionListener<SearchResponse> done = new ActionListener<>() {
            // a search function may complete the listener and then still throw, only the first outcome counts
            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void onResponse(SearchResponse response) {
                DataState now;
                try {
                    now = DataState.of(response);
                } catch (Exception e) {
                    onFailure(e);
                    return;
                }
                if (now == null) {
                    onFailure(new IllegalStateException("the search did not tell the state of the data"));
                    return;
                }
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                counts[stored.sameAs(now) ? 0 : 1]++;
                next(version, records, unreadable, index + 1, counts, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                logger.debug("failed to look at the state of the data of a golden query", e);
                counts[3]++; // neither fresh nor stale: it could not be told
                next(version, records, unreadable, index + 1, counts, listener);
            }
        };
        try {
            probe.accept(DataState.probe(record.search().query()), done);
        } catch (Exception e) {
            done.onFailure(e);
        }
    }
}
