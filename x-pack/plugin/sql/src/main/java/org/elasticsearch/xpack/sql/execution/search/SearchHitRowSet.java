/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Extracts rows from an array of {@link SearchHit}.
 */
class SearchHitRowSet extends ResultRowSet<HitExtractor> {
    private final SearchHit[] hits;
    private final Map<SearchHit, Map<String, SearchHit[]>> flatInnerHits = new HashMap<>();
    private final Set<String> innerHits = new LinkedHashSet<>();
    private final String innerHit;

    private final int size;
    private final int[] indexPerLevel;
    private final int remainingLimit;

    private int row = 0;

    SearchHitRowSet(List<HitExtractor> exts, BitSet mask, int sizeRequested, int limit, SearchResponse response) {
        super(exts, mask);

        this.hits = response.getHits().getHits();

        // Since the results might contain nested docs, the iteration is similar to that of Aggregation
        // namely it discovers the nested docs and then, for iteration, increments the deepest level first
        // and eventually carries that over to the top level

        String innerHit = null;
        for (HitExtractor ex : exts) {
            if (ex.hitName() != null) {
                innerHits.add(ex.hitName());
                if (innerHit == null) {
                    innerHit = ex.hitName();
                }
            }
        }

        int sz = hits.length;

        int maxDepth = 0;
        if (innerHits.isEmpty() == false) {
            if (innerHits.size() > 1) {
                throw new SqlIllegalArgumentException("Multi-nested docs not yet supported {}", innerHits);
            }
            maxDepth = 1;

            sz = 0;
            for (SearchHit hit : hits) {
                Map<String, SearchHit[]> innerHitsPerPath = new HashMap<>(innerHits.size());
                for (String ih : innerHits) {
                    SearchHit[] sh = getAllInnerHits(hit, ih);
                    innerHitsPerPath.put(ih, sh);
                    sz += sh.length;
                }
                flatInnerHits.put(hit, innerHitsPerPath);
            }
        }
        // page size
        size = limit < 0 ? sz : Math.min(sz, limit);
        indexPerLevel = new int[maxDepth + 1];
        this.innerHit = innerHit;

        // compute remaining limit (only if the limit is specified - that is, positive).
        int remaining = limit < 0 ? limit : limit - size;
        // either the search returned fewer records than requested or the limit is exhausted
        if (size < sizeRequested || remaining == 0) {
            remainingLimit = 0;
        } else {
            remainingLimit = remaining;
        }
    }

    public boolean hasRemaining() {
        return remainingLimit != 0;
    }

    public int getRemainingLimit() {
        return remainingLimit;
    }

    @Override
    protected Object extractValue(HitExtractor e) {
        int extractorLevel = e.hitName() == null ? 0 : 1;

        SearchHit hit = null;
        SearchHit[] sh = hits;
        for (int lvl = 0; lvl <= extractorLevel; lvl++) {
            // TODO: add support for multi-nested doc
            if (hit != null) {
                SearchHit[] innerHits = flatInnerHits.get(hit).get(innerHit);
                sh = innerHits == null ? SearchHits.EMPTY : innerHits;
            }
            hit = sh[indexPerLevel[lvl]];
        }

        return e.extract(hit);
    }

    private SearchHit[] getAllInnerHits(SearchHit hit, String path) {
        if (hit == null) {
            return null;
        }

        // multiple inner_hits results sections can match the same nested documents, thus we eliminate the duplicates by
        // using the offset as the "deduplicator" in a HashMap
        HashMap<Integer, SearchHit> lhm = new HashMap<>();
        for (Entry<String, SearchHits> entry : hit.getInnerHits().entrySet()) {
            int endOfPath = entry.getKey().lastIndexOf('_');
            if (endOfPath >= 0 && entry.getKey().substring(0, endOfPath).equals(path)) {
                SearchHit[] h = entry.getValue().getHits();
                for (SearchHit element : h) {
                    lhm.put(element.getNestedIdentity().getOffset(), element);
                }
            }
        }

        // Then sort the resulting List based on the offset of the same inner hit. Each inner_hit match will have an offset value,
        // relative to its location in the _source
        List<SearchHit> sortedList = new ArrayList<>(lhm.values());
        Collections.sort(sortedList, new NestedHitOffsetComparator());

        return sortedList.toArray(SearchHit[]::new);
    }

    private class NestedHitOffsetComparator implements Comparator<SearchHit> {
        @Override
        public int compare(SearchHit sh1, SearchHit sh2) {
            if (sh1 == null && sh2 == null) {
                return 0;
            } else if (sh1 == null) {
                return -1;
            } else if (sh2 == null) {
                return 1;
            }

            return Integer.valueOf(sh1.getNestedIdentity().getOffset()).compareTo(Integer.valueOf(sh2.getNestedIdentity().getOffset()));
        }
    }

    @Override
    protected boolean doHasCurrent() {
        return row < size;
    }

    @Override
    protected boolean doNext() {
        if (row < size - 1) {
            row++;
            // increment last row
            indexPerLevel[indexPerLevel.length - 1]++;
            // then check size
            SearchHit[] sh = hits;
            for (int lvl = 0; lvl < indexPerLevel.length; lvl++) {
                if (indexPerLevel[lvl] == sh.length) {
                    // reset the current branch
                    indexPerLevel[lvl] = 0;
                    // bump the parent - if it's too big it, the loop will restart again from that position
                    indexPerLevel[lvl - 1]++;
                    // restart the loop
                    lvl = 0;
                    sh = hits;
                } else {
                    SearchHit h = sh[indexPerLevel[lvl]];
                    // TODO: improve this for multi-nested responses
                    String path = lvl == 0 ? innerHit : null;
                    if (path != null) {
                        SearchHit[] innerHits = flatInnerHits.get(h).get(path);
                        sh = innerHits == null ? SearchHits.EMPTY : innerHits;
                    }
                }
            }

            return true;
        }
        return false;
    }

    @Override
    protected void doReset() {
        row = 0;
        Arrays.fill(indexPerLevel, 0);
    }

    @Override
    public int size() {
        return size;
    }
}
