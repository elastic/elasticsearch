/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.DataType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SearchStats {

    private final List<SearchContext> contexts;

    private static class FieldStat {
        private Long count;
        private Boolean exists;
        private Object min, max;
    }

    private static final int CACHE_SIZE = 32;

    // simple non-thread-safe cache for avoiding unnecessary IO (which while fast it still I/O)
    private final Map<String, FieldStat> cache = new LinkedHashMap<>(CACHE_SIZE, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, FieldStat> eldest) {
            return size() > CACHE_SIZE;
        }
    };

    public SearchStats(List<SearchContext> contexts) {
        this.contexts = contexts;
    }

    public long count() {
        var count = new long[] { 0 };
        boolean completed = doWithContexts(r -> count[0] += r.numDocs(), false);
        return completed ? count[0] : -1;
    }

    public long count(String field) {
        var stat = cache.computeIfAbsent(field, s -> new FieldStat());
        if (stat.count == null) {
            var count = new long[] { 0 };
            boolean completed = doWithContexts(r -> count[0] += countEntries(r, field), false);
            stat.count = completed ? count[0] : -1;
        }
        return stat.count;
    }

    public long count(String field, BytesRef value) {
        var count = new long[] { 0 };
        Term term = new Term(field, value);
        boolean completed = doWithContexts(r -> count[0] += r.docFreq(term), false);
        return completed ? count[0] : -1;
    }

    public boolean exists(String field) {
        var stat = cache.computeIfAbsent(field, s -> new FieldStat());
        if (stat.exists == null) {
            stat.exists = false;
            // even if there are deleted documents, check the existence of a field
            // since if it's missing, deleted documents won't change that
            for (SearchContext context : contexts) {
                if (context.getSearchExecutionContext().isFieldMapped(field)) {
                    stat.exists = true;
                    break;
                }
            }
        }
        return stat.exists;
    }

    public byte[] min(String field, DataType dataType) {
        var stat = cache.computeIfAbsent(field, s -> new FieldStat());
        if (stat.min == null) {
            var min = new byte[][] { null };
            doWithContexts(r -> {
                byte[] localMin = PointValues.getMinPackedValue(r, field);
                // TODO: how to compare with the previous min
                if (localMin != null) {
                    if (min[0] == null) {
                        min[0] = localMin;
                    } else {
                        throw new EsqlIllegalArgumentException("Don't know how to compare with previous min");
                    }
                }

            }, true);
            stat.min = min[0];
        }
        // return stat.min;
        return null;
    }

    public byte[] max(String field, DataType dataType) {
        var stat = cache.computeIfAbsent(field, s -> new FieldStat());
        if (stat.max == null) {

            var max = new byte[][] { null };
            doWithContexts(r -> {
                byte[] localMax = PointValues.getMaxPackedValue(r, field);
                // TODO: how to compare with the previous max
                if (localMax != null) {
                    if (max[0] == null) {
                        max[0] = localMax;
                    } else {
                        throw new EsqlIllegalArgumentException("Don't know how to compare with previous max");
                    }
                }
            }, true);
            stat.max = max[0];
        }
        // return stat.max;
        return null;
    }

    //
    // @see org.elasticsearch.search.query.QueryPhaseCollectorManager#shortcutTotalHitCount(IndexReader, Query)
    //
    private static int countEntries(IndexReader indexReader, String field) {
        int count = 0;
        try {
            for (LeafReaderContext context : indexReader.leaves()) {
                LeafReader reader = context.reader();
                FieldInfos fieldInfos = reader.getFieldInfos();
                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

                if (fieldInfo != null) {
                    if (fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                        // no shortcut possible: it's a text field, empty values are counted as no value.
                        return -1;
                    }
                    if (fieldInfo.getPointIndexDimensionCount() > 0) {
                        PointValues points = reader.getPointValues(field);
                        if (points != null) {
                            count += points.getDocCount();
                        }
                    } else if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                        Terms terms = reader.terms(field);
                        if (terms != null) {
                            count += terms.getDocCount();
                        }
                    } else {
                        return -1; // no shortcut possible for fields that are not indexed
                    }
                }
            }
        } catch (IOException ex) {
            throw new EsqlIllegalArgumentException("Cannot access data storage", ex);
        }
        return count;
    }

    private interface IndexReaderConsumer {
        void consume(IndexReader reader) throws IOException;
    }

    private boolean doWithContexts(IndexReaderConsumer consumer, boolean acceptsDeletions) {
        try {
            for (SearchContext context : contexts) {
                for (LeafReaderContext leafContext : context.searcher().getLeafContexts()) {
                    var reader = leafContext.reader();
                    if (acceptsDeletions == false && reader.hasDeletions()) {
                        return false;
                    }
                    consumer.consume(reader);
                }
            }
            return true;
        } catch (IOException ex) {
            throw new EsqlIllegalArgumentException("Cannot access data storage", ex);
        }
    }
}
