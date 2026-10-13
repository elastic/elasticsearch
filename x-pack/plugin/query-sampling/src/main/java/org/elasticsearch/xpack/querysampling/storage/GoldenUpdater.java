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
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Brings the golden dataset up to date by a version: the latest complete version, the stored sampled queries that are not in it yet
 * and, if it was set up to, the ground truth of its queries whose data changed make a new one. The version it is made from is not
 * touched, as whatever was run on it can be run on it again.
 * <ul>
 * <li>Queries of the version whose ground truth is good are copied as they are.</li>
 * <li>Queries whose data changed, and those that were stored without the state of the data, which cannot be told, have their
 * exact search run again. A query that could not be looked at or computed is left out of the new version and reported: its old
 * ground truth cannot be vouched for.</li>
 * <li>Stored sampled queries that have ground truth and are not in the version are added. Arrivals that were kept as events are
 * not, a golden query is a query and not one of its searches.</li>
 * </ul>
 * The dataset is not to grow without limit, so a version has at most the number of queries it is given: when there are more, those
 * that were picked longest ago are left out. If nothing was computed again and nothing is new, no version is made, as it would be a
 * copy of the one there is.
 * <p>
 * Looking at the data and computing again are searches over the data of the users, done as whoever asked, one after the other. An
 * updater that was made without them only adds.
 */
public final class GoldenUpdater {

    /**
     * The most that one search of an index can return.
     */
    private static final int MAX_READ = 10_000;

    /**
     * @param sourceVersion the version that was brought up to date, 0 if there was none
     * @param version       the version that was made, 0 if there was nothing to change and none was made
     * @param copied        queries of the source version whose ground truth was still good, which are in the new version as they were
     * @param recomputed    queries of the source version whose ground truth was computed again, which are in the new version
     * @param added         queries that were not in the source version and are in the new one
     * @param dropped       queries that would have made the version larger than its limit, which are left out
     * @param failed        queries that are not in the new version because they could not be read, looked at or computed, and
     *                      queries that could not be written, which are also counted in the group they were in
     */
    public record Result(long sourceVersion, long version, int copied, int recomputed, int added, int dropped, int failed) {}

    private final GoldenReader reader;
    private final StoredSamples samples;
    private final GoldenWriter writer;
    private final GoldenStaleness staleness;
    private final GroundTruthRunner runner;

    /**
     * @param reader    reads the golden dataset, as the plugin
     * @param samples   reads the sampled queries, as the plugin
     * @param writer    writes the new version, as the plugin
     * @param staleness looks at the data of the queries, as the caller, or {@code null} to leave the ground truth as it is
     * @param exact     runs the exact searches, as the caller, or {@code null} if there is no staleness
     */
    public GoldenUpdater(
        GoldenReader reader,
        StoredSamples samples,
        GoldenWriter writer,
        @Nullable GoldenStaleness staleness,
        @Nullable BiConsumer<SearchRequest, ActionListener<SearchResponse>> exact
    ) {
        assert (staleness == null) == (exact == null) : "looking at the data and computing again go together";
        this.reader = reader;
        this.samples = samples;
        this.writer = writer;
        this.staleness = staleness;
        this.runner = exact == null ? null : new GroundTruthRunner(exact);
    }

    /**
     * @param limit the most queries of the version that is made
     */
    public void update(int limit, ActionListener<Result> listener) {
        reader.latestCompleted(ActionListener.wrap(latest -> {
            if (latest == 0) {
                candidates(latest, new Existing(List.of(), Map.of(), List.of(), 0, 0, 0), limit, listener);
            } else {
                reader.read(latest, limit, ActionListener.wrap(read -> refresh(latest, read, limit, listener), listener::onFailure));
            }
        }, listener::onFailure));
    }

    /**
     * What is left of the queries of the source version once their ground truth was looked at.
     *
     * @param all        every query that was read, which are not added again
     * @param recomputed the queries that have a ground truth of today, by the query they replace
     * @param kept       the queries that are in the next version: those that are fresh and those that were computed again
     */
    private record Existing(
        List<StoredSample> all,
        Map<StoredSample, StoredSample> recomputed,
        List<StoredSample> kept,
        int copied,
        int recomputedCount,
        int failed
    ) {}

    private void refresh(long latest, GoldenReader.Read read, int limit, ActionListener<Result> listener) {
        List<StoredSample> records = read.records();
        if (staleness == null || records.isEmpty()) {
            candidates(latest, new Existing(records, Map.of(), records, records.size(), 0, read.unreadable()), limit, listener);
            return;
        }
        staleness.classify(records, ActionListener.wrap(statuses -> {
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
                ActionListener.wrap(result -> candidates(latest, existing(read, statuses, computed), limit, listener), listener::onFailure)
            );
        }, listener::onFailure));
    }

    private static Existing existing(
        GoldenReader.Read read,
        List<GoldenStaleness.Status> statuses,
        Map<StoredSample, GroundTruth> computed
    ) {
        List<StoredSample> records = read.records();
        List<StoredSample> kept = new ArrayList<>();
        Map<StoredSample, StoredSample> recomputed = new IdentityHashMap<>();
        int copied = 0;
        int failed = read.unreadable();
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
        return new Existing(records, recomputed, kept, copied, recomputed.size(), failed);
    }

    private void candidates(long latest, Existing existing, int limit, ActionListener<Result> listener) {
        samples.read(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("has_ground_truth", true))
                .mustNot(QueryBuilders.existsQuery("event_id")),
            "picked_at",
            SortOrder.DESC,
            // enough to get past the queries that are in the version already, which are the most of the newest if nothing happened
            Math.min(MAX_READ, limit + existing.all().size()),
            ActionListener.wrap(read -> merge(latest, existing, oncePerQuery(read.samples()), limit, listener), listener::onFailure)
        );
    }

    /**
     * The most recent of the stored queries of each fingerprint, in the order they came.
     */
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
        // the newest are the ones to keep
        all.sort(Comparator.comparingLong(StoredSample::pickedAt).reversed());
        int dropped = Math.max(0, all.size() - limit);
        List<StoredSample> kept = new ArrayList<>(all.subList(0, all.size() - dropped));
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
            // nothing is new or computed again, so the version would be a copy, or a smaller one of the version there is
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
