/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Grows the golden dataset by a version: the latest complete version and the stored sampled queries that have ground truth and are
 * not in it yet make a new one. The ground truth of what is kept is not touched, which is for a refresh to do when the data changes.
 * <p>
 * The dataset is not to grow without limit, so a version has at most the number of queries that it is given. When there are more,
 * those that were picked longest ago are left out. If there is nothing new, no version is made, as it would be a copy of the one
 * there is. Arrivals that were kept as events are not promoted, a golden query is a query and not one of its searches.
 */
public final class GoldenGrower {

    /**
     * The most that one search of an index can return.
     */
    private static final int MAX_READ = 10_000;

    /**
     * @param sourceVersion the version that was grown, 0 if there was none
     * @param version       the version that was made, 0 if there was nothing new and none was made
     * @param added         queries that were not in the source version and are in the new one
     * @param dropped       queries that would have made the version larger than its limit, which are left out
     * @param failed        queries that could not be read, and queries that could not be written
     */
    public record Result(long sourceVersion, long version, int added, int dropped, int failed) {}

    private final GoldenReader reader;
    private final StoredSamples samples;
    private final GoldenWriter writer;

    /**
     * @param reader  reads the golden dataset, as the plugin
     * @param samples reads the sampled queries, as the plugin
     * @param writer  writes the new version, as the plugin
     */
    public GoldenGrower(GoldenReader reader, StoredSamples samples, GoldenWriter writer) {
        this.reader = reader;
        this.samples = samples;
        this.writer = writer;
    }

    /**
     * @param limit the most queries of the version that is made
     */
    public void grow(int limit, ActionListener<Result> listener) {
        reader.latestCompleted(ActionListener.wrap(latest -> {
            if (latest == 0) {
                candidates(0, new GoldenReader.Read(List.of(), 0), limit, listener);
            } else {
                reader.read(
                    latest,
                    limit,
                    ActionListener.wrap(existing -> candidates(latest, existing, limit, listener), listener::onFailure)
                );
            }
        }, listener::onFailure));
    }

    private void candidates(long latest, GoldenReader.Read existing, int limit, ActionListener<Result> listener) {
        samples.read(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("has_ground_truth", true))
                .mustNot(QueryBuilders.existsQuery("event_id")),
            "picked_at",
            SortOrder.DESC,
            // enough to get past the queries that are in the version already, which are the most of the newest if nothing happened
            Math.min(MAX_READ, limit + existing.records().size()),
            ActionListener.wrap(
                read -> grow(latest, existing, GoldenPromoter.oncePerQuery(read.samples()), limit, listener),
                listener::onFailure
            )
        );
    }

    private void grow(long latest, GoldenReader.Read existing, List<StoredSample> candidates, int limit, ActionListener<Result> listener) {
        Set<String> known = new HashSet<>();
        existing.records().forEach(record -> known.add(record.fingerprint()));
        List<StoredSample> added = new ArrayList<>();
        for (StoredSample candidate : candidates) {
            if (known.contains(candidate.fingerprint()) == false) {
                added.add(candidate);
            }
        }
        if (added.isEmpty()) {
            listener.onResponse(new Result(latest, 0, 0, 0, existing.unreadable()));
            return;
        }
        List<StoredSample> all = new ArrayList<>(existing.records());
        all.addAll(added);
        // the newest are the ones to keep
        all.sort(Comparator.comparingLong(StoredSample::pickedAt).reversed());
        int dropped = Math.max(0, all.size() - limit);
        List<StoredSample> kept = new ArrayList<>(all.subList(0, all.size() - dropped));
        Set<StoredSample> newOnes = Collections.newSetFromMap(new IdentityHashMap<>());
        newOnes.addAll(added);
        int addedKept = (int) kept.stream().filter(newOnes::contains).count();
        if (addedKept == 0) {
            // all that is new is older than all that is there, and the version is full: it would not change
            listener.onResponse(new Result(latest, 0, 0, dropped, existing.unreadable()));
            return;
        }
        int unreadable = existing.unreadable();
        writer.write(
            kept,
            latest == 0 ? null : latest,
            listener.map(written -> new Result(latest, written.version(), addedKept, dropped, unreadable + written.failed()))
        );
    }
}
