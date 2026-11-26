/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class FetchedHits {

    private FetchedHits() {}

    public static <T> int dropIncomplete(List<T> matches, List<List<SearchHit>> hits, List<T> keptMatches, List<List<SearchHit>> keptHits) {
        int dropped = 0;
        for (int i = 0; i < matches.size(); i++) {
            List<SearchHit> row = hits.get(i);
            if (missingHit(row)) {
                release(row);
                dropped++;
            } else {
                keptMatches.add(matches.get(i));
                keptHits.add(row);
            }
        }
        return dropped;
    }

    public static List<ShardSearchFailure> reportDrops(Collection<ShardSearchFailure> existing, int dropped) {
        if (dropped == 0) {
            return List.copyOf(existing);
        }
        List<ShardSearchFailure> failures = new ArrayList<>(existing);
        failures.add(
            new ShardSearchFailure(new IllegalStateException("Dropped [" + dropped + "] matches because a document could not be fetched"))
        );
        return List.copyOf(failures);
    }

    public static void releaseRows(List<List<SearchHit>> rows) {
        for (List<SearchHit> row : rows) {
            release(row);
        }
    }

    private static boolean missingHit(List<SearchHit> row) {
        for (SearchHit hit : row) {
            if (hit == null) {
                return true;
            }
        }
        return false;
    }

    private static void release(List<SearchHit> row) {
        for (SearchHit hit : row) {
            if (hit != null) {
                hit.decRef();
            }
        }
    }
}
