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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.querysampling.GroundTruthRunner;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * Makes a new golden version from the latest complete one: fresh queries are copied, stale or unknown ones get their ground truth
 * recomputed (left out and reported if that fails), and new sampled queries with ground truth are added. Event arrivals are not
 * added, as a golden query is a query and not one of its searches. The source version is never touched, and when there are more
 * than the limit the oldest picks are left out. No version is made if nothing was recomputed or added. Staleness checks and
 * recomputation search user data as the caller; an updater built without them only adds.
 */
public final class GoldenUpdater {

    // the most that one search of an index can return
    private static final int MAX_READ = QuerySamplingSettings.MAX_RESULT_WINDOW;

    /** {@code version} is 0 if nothing changed and none was made; {@code failed} counts queries that were left out or not written. */
    public record Result(long sourceVersion, long version, int copied, int recomputed, int added, int dropped, int failed) {}

    private final GoldenReader reader;
    private final StoredSamples samples;
    private final GoldenWriter writer;
    private final GoldenStaleness staleness;
    private final GroundTruthRunner runner;

    /** A null {@code staleness} (and {@code exact} and {@code executor}) leaves the ground truth as it is. */
    public GoldenUpdater(
        GoldenReader reader,
        StoredSamples samples,
        GoldenWriter writer,
        @Nullable GoldenStaleness staleness,
        @Nullable BiConsumer<SearchRequest, ActionListener<SearchResponse>> exact,
        @Nullable Executor executor
    ) {
        assert (staleness == null) == (exact == null) : "looking at the data and computing again go together";
        assert (exact == null) == (executor == null) : "the exact searches are started on the executor";
        this.reader = reader;
        this.samples = samples;
        this.writer = writer;
        this.staleness = staleness;
        this.runner = exact == null ? null : new GroundTruthRunner(exact, executor);
    }

    public void update(int limit, ActionListener<Result> listener) {
        reader.latestCompleted(ActionListener.wrap(latest -> {
            if (latest == 0) {
                candidates(latest, new Existing(List.of(), Map.of(), List.of(), 0, 0, 0, 0), limit, listener);
            } else {
                // read all of it: the read order does not tell which queries are the newest
                reader.read(latest, MAX_READ, ActionListener.wrap(read -> refresh(latest, read, limit, listener), listener::onFailure));
            }
        }, listener::onFailure));
    }

    /**
     * @param all     every query read, which are not added again
     * @param kept    the fresh and the recomputed queries
     * @param trimmed older queries not looked at because the next version cannot hold them
     */
    private record Existing(
        List<StoredSample> all,
        Map<StoredSample, StoredSample> recomputed,
        List<StoredSample> kept,
        int copied,
        int recomputedCount,
        int failed,
        int trimmed
    ) {}

    private void refresh(long latest, StoredSamples.Read read, int limit, ActionListener<Result> listener) {
        // the next version keeps the newest queries, so the older ones are not looked at or recomputed
        List<StoredSample> all = read.samples();
        List<StoredSample> records = new ArrayList<>(all);
        records.sort(Comparator.comparingLong(StoredSample::pickedAt).reversed());
        int trimmed = Math.max(0, records.size() - limit);
        records = new ArrayList<>(records.subList(0, records.size() - trimmed));
        if (staleness == null || records.isEmpty()) {
            candidates(latest, new Existing(all, Map.of(), records, records.size(), 0, read.unreadable(), trimmed), limit, listener);
            return;
        }
        List<StoredSample> looked = records;
        staleness.classify(looked, ActionListener.wrap(statuses -> {
            List<StoredSample> toCompute = new ArrayList<>();
            for (int i = 0; i < looked.size(); i++) {
                if (statuses.get(i) == GoldenStaleness.Status.STALE || statuses.get(i) == GoldenStaleness.Status.UNKNOWN) {
                    toCompute.add(looked.get(i));
                }
            }
            Map<StoredSample, GroundTruth> computed = new IdentityHashMap<>();
            runner.run(
                toCompute,
                sample -> sample.search().query(),
                computed::put,
                ActionListener.wrap(
                    result -> candidates(latest, existing(all, looked, read.unreadable(), trimmed, statuses, computed), limit, listener),
                    listener::onFailure
                )
            );
        }, listener::onFailure));
    }

    private static Existing existing(
        List<StoredSample> all,
        List<StoredSample> records,
        int unreadable,
        int trimmed,
        List<GoldenStaleness.Status> statuses,
        Map<StoredSample, GroundTruth> computed
    ) {
        List<StoredSample> kept = new ArrayList<>();
        Map<StoredSample, StoredSample> recomputed = new IdentityHashMap<>();
        int copied = 0;
        int failed = unreadable;
        for (int i = 0; i < records.size(); i++) {
            StoredSample record = records.get(i);
            switch (statuses.get(i)) {
                case FRESH -> {
                    kept.add(record);
                    copied++;
                }
                case STALE, UNKNOWN -> {
                    GroundTruth groundTruth = computed.get(record);
                    if (groundTruth == null) {
                        failed++;
                    } else {
                        StoredSample again = withGroundTruth(record, groundTruth);
                        recomputed.put(again, record);
                        kept.add(again);
                    }
                }
                case FAILED -> failed++;
            }
        }
        return new Existing(all, recomputed, kept, copied, recomputed.size(), failed, trimmed);
    }

    private void candidates(long latest, Existing existing, int limit, ActionListener<Result> listener) {
        samples.read(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("has_ground_truth", true))
                .mustNot(QueryBuilders.existsQuery("event_id")),
            "picked_at",
            SortOrder.DESC,
            // enough to get past the queries already in the version
            Math.min(MAX_READ, limit + existing.all().size()),
            ActionListener.wrap(read -> merge(latest, existing, oncePerQuery(read.samples()), limit, listener), listener::onFailure)
        );
    }

    static List<StoredSample> oncePerQuery(List<StoredSample> stored) {
        Set<String> seen = new HashSet<>();
        List<StoredSample> once = new ArrayList<>();
        for (StoredSample sample : stored) {
            if (seen.add(sample.fingerprint())) {
                once.add(sample);
            }
        }
        return once;
    }

    private void merge(long latest, Existing existing, List<StoredSample> candidates, int limit, ActionListener<Result> listener) {
        Set<String> known = new HashSet<>();
        existing.all().forEach(record -> known.add(record.fingerprint()));
        List<StoredSample> added = new ArrayList<>();
        for (StoredSample candidate : candidates) {
            if (known.contains(candidate.fingerprint()) == false) {
                added.add(candidate);
            }
        }
        List<StoredSample> all = new ArrayList<>(existing.kept());
        all.addAll(added);
        all.sort(Comparator.comparingLong(StoredSample::pickedAt).reversed());
        int overflow = Math.max(0, all.size() - limit);
        int dropped = overflow + existing.trimmed();
        List<StoredSample> kept = new ArrayList<>(all.subList(0, all.size() - overflow));
        Set<StoredSample> addedOnes = Collections.newSetFromMap(new IdentityHashMap<>());
        addedOnes.addAll(added);
        int addedKept = 0;
        int recomputedKept = 0;
        for (StoredSample sample : kept) {
            if (addedOnes.contains(sample)) {
                addedKept++;
            } else if (existing.recomputed().containsKey(sample)) {
                recomputedKept++;
            }
        }
        int copiedKept = kept.size() - addedKept - recomputedKept;
        if (addedKept == 0 && recomputedKept == 0) {
            // it would only be a copy, or a smaller one, of the existing version
            listener.onResponse(new Result(latest, 0, copiedKept, 0, 0, dropped, existing.failed()));
            return;
        }
        int failed = existing.failed();
        int copied = copiedKept;
        int recomputed = recomputedKept;
        int addedCount = addedKept;
        writer.write(
            kept,
            latest == 0 ? null : latest,
            listener.map(
                written -> new Result(latest, written.version(), copied, recomputed, addedCount, dropped, failed + written.failed())
            )
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
