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
import org.elasticsearch.xpack.querysampling.GroundTruthRunner;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Makes a new version of the golden dataset from an earlier one, in which the ground truth of the queries whose data changed is
 * computed again. The earlier version is not touched: the point of versions is that whatever was run on one can be run on it
 * again.
 * <p>
 * Every query of the version is looked at first, with the same search that checks for staleness. Those that are fresh are
 * copied as they are. Those that are stale, and those that were stored without the state of the data, which cannot be told,
 * have their exact search run again. A query that could not be looked at or whose ground truth could not be computed is
 * left out of the new version, and reported: its old ground truth cannot be vouched for, and a new version is not to hold it.
 * If there is nothing to compute again no version is made, as it would be a copy of the one there is.
 * <p>
 * The searches are done one after the other, as whoever asked, so that they only see what that person may see.
 */
public final class GoldenRefresher {

    /**
     * @param sourceVersion the version that was refreshed, 0 if there is none
     * @param version       the version that was made, 0 if there was nothing to compute again and none was made
     * @param copied        queries whose ground truth was still good, which are in the new version as they were
     * @param recomputed    queries whose ground truth was computed again, which are in the new version
     * @param failed        queries that are not in the new version, because they could not be read, looked at or computed, and
     *                      queries that could not be written, which are also counted in {@code copied} or {@code recomputed}
     *                      as the writer does not say which they were
     */
    public record Result(long sourceVersion, long version, int copied, int recomputed, int failed) {}

    private final GoldenReader reader;
    private final GoldenStaleness staleness;
    private final GroundTruthRunner runner;
    private final GoldenWriter writer;

    /**
     * @param reader    reads the golden dataset, as the plugin
     * @param staleness looks at the data of the queries, as the caller
     * @param exact     runs the exact searches, as the caller
     * @param writer    writes the new version, as the plugin
     */
    public GoldenRefresher(
        GoldenReader reader,
        GoldenStaleness staleness,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> exact,
        GoldenWriter writer
    ) {
        this.reader = reader;
        this.staleness = staleness;
        this.runner = new GroundTruthRunner(exact);
        this.writer = writer;
    }

    /**
     * Refreshes up to {@code max} queries of a version.
     *
     * @param version the version, or 0 for the latest that is complete
     */
    public void refresh(long version, int max, ActionListener<Result> listener) {
        if (version > 0) {
            read(version, max, listener);
            return;
        }
        reader.latestCompleted(ActionListener.wrap(latest -> {
            if (latest == 0) {
                listener.onResponse(new Result(0, 0, 0, 0, 0));
            } else {
                read(latest, max, listener);
            }
        }, listener::onFailure));
    }

    private void read(long version, int max, ActionListener<Result> listener) {
        reader.read(
            version,
            max,
            ActionListener.wrap(
                read -> staleness.classify(
                    read.records(),
                    ActionListener.wrap(statuses -> recompute(version, read, statuses, listener), listener::onFailure)
                ),
                listener::onFailure
            )
        );
    }

    private void recompute(long version, GoldenReader.Read read, List<GoldenStaleness.Status> statuses, ActionListener<Result> listener) {
        List<StoredSample> records = read.records();
        List<StoredSample> toCompute = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            if (statuses.get(i) == GoldenStaleness.Status.STALE || statuses.get(i) == GoldenStaleness.Status.UNKNOWN) {
                toCompute.add(records.get(i));
            }
        }
        Map<StoredSample, GroundTruth> computed = new IdentityHashMap<>();
        runner.run(
            toCompute,
            sample -> sample.search().query(),
            computed::put,
            ActionListener.wrap(result -> write(version, read, statuses, computed, listener), listener::onFailure)
        );
    }

    private void write(
        long version,
        GoldenReader.Read read,
        List<GoldenStaleness.Status> statuses,
        Map<StoredSample, GroundTruth> computed,
        ActionListener<Result> listener
    ) {
        List<StoredSample> records = read.records();
        List<StoredSample> next = new ArrayList<>();
        int copied = 0;
        int failed = read.unreadable();
        for (int i = 0; i < records.size(); i++) {
            StoredSample record = records.get(i);
            switch (statuses.get(i)) {
                case FRESH -> {
                    next.add(record);
                    copied++;
                }
                case STALE, UNKNOWN -> {
                    GroundTruth groundTruth = computed.get(record);
                    if (groundTruth == null) {
                        failed++;
                    } else {
                        next.add(withGroundTruth(record, groundTruth));
                    }
                }
                case FAILED -> failed++;
            }
        }
        int recomputed = next.size() - copied;
        if (recomputed == 0) {
            listener.onResponse(new Result(version, 0, copied, 0, failed));
            return;
        }
        int notComputed = failed;
        int copiedCount = copied;
        writer.write(
            next,
            version,
            listener.map(written -> new Result(version, written.version(), copiedCount, recomputed, notComputed + written.failed()))
        );
    }

    private static StoredSample withGroundTruth(StoredSample sample, GroundTruth groundTruth) {
        return new StoredSample(
            sample.samplerId(),
            sample.fingerprint(),
            sample.search(),
            sample.weights(),
            sample.pickedAt(),
            sample.updatedAt(),
            groundTruth,
            sample.stratum(),
            sample.hardness(),
            sample.eventId(),
            sample.selectivity()
        );
    }
}
