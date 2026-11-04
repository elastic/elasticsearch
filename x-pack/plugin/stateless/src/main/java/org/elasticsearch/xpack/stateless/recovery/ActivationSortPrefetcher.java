/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.io.IOException;
import java.util.List;

/**
 * Fast activation prototype (phase 5, default off: {@link FastSearchActivationSettings#FAST_ACTIVATION_PREFETCH_SORT_DOC_VALUES_SETTING}):
 * before a query sorted by field runs on a search shard that is still in its activation fill window (freshly activated, cold cache),
 * creates the doc values of every sort field on every segment. Creating numeric/binary doc values makes the codec prefetch the first page
 * of their data ({@code Lucene90DocValuesProducer#getNumeric} and friends), so the doc-values reads of all segments go out in one wave.
 * <p>
 * Without this, field-sort collection visits the segments one after the other (Elasticsearch disables parallel collection for field
 * sorts, {@code FieldSortBuilder#supportsParallelCollection}), each segment's doc values are created (and prefetched) only when the
 * collector reaches it, and the first read follows immediately: one object store round trip per segment on a cold shard.
 * <p>
 * Allowing parallel collection instead was not chosen: the query phase groups small segments into slices of at least
 * {@code search.query_phase_min_docs_per_slice}-like sizes, so the segments of a slice would still be read one after the other; it would
 * also change CPU use and the sort-optimization behaviour of every field-sorted query, while this only issues non-blocking prefetches.
 */
public final class ActivationSortPrefetcher implements SearchOperationListener {

    private static final Logger logger = LogManager.getLogger(ActivationSortPrefetcher.class);

    private final ClusterSettings clusterSettings;

    public ActivationSortPrefetcher(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        if (FastSearchActivationSettings.getOrDefault(
            clusterSettings,
            FastSearchActivationSettings.FAST_ACTIVATION_PREFETCH_SORT_DOC_VALUES_SETTING
        ) == false) {
            return;
        }
        final SortAndFormats sort = searchContext.sort();
        if (sort == null) {
            return;
        }
        if (inActivationFillWindow(searchContext.indexShard().store().directory()) == false) {
            return;
        }
        try {
            prefetch(searchContext.searcher().getIndexReader().leaves(), sort.sort.getSort());
        } catch (Exception e) {
            logger.debug(() -> "failed to prefetch sort doc values of " + searchContext.indexShard().shardId(), e);
        }
    }

    private static boolean inActivationFillWindow(Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof SearchDirectory searchDirectory) {
                return searchDirectory.inActivationFillWindow();
            }
            dir = dir instanceof FilterDirectory filterDirectory ? filterDirectory.getDelegate() : null;
        }
        return false;
    }

    /**
     * Creates the doc values of each sort field (by its doc values type) on every leaf. Returns the number of doc values created.
     */
    static int prefetch(List<LeafReaderContext> leaves, SortField[] sortFields) throws IOException {
        int created = 0;
        for (SortField sortField : sortFields) {
            final String field = sortField.getField();
            if (field == null) {
                continue; // score, doc
            }
            for (LeafReaderContext leaf : leaves) {
                final LeafReader reader = leaf.reader();
                final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
                if (fieldInfo == null) {
                    continue;
                }
                final DocValuesType type = fieldInfo.getDocValuesType();
                switch (type) {
                    case NUMERIC -> reader.getNumericDocValues(field);
                    case SORTED_NUMERIC -> reader.getSortedNumericDocValues(field);
                    case SORTED -> reader.getSortedDocValues(field);
                    case SORTED_SET -> reader.getSortedSetDocValues(field);
                    case BINARY -> reader.getBinaryDocValues(field);
                    case NONE -> {
                        continue;
                    }
                }
                created++;
            }
        }
        return created;
    }
}
