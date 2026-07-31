/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 * A range predicate over one date field that {@link ShardResultCacheKey} pulled out of the query fingerprint so it can
 * be resolved per shard instead. {@code from} and {@code to} are epoch millis; either may be absent for a half-open
 * range.
 * <p>
 * Relative-time predicates are the reason this exists. {@code NOW()} folds to a full-precision literal at the
 * coordinator, so {@code WHERE @timestamp >= NOW() - 24h} ships a different fragment every time it runs and would
 * produce a fresh key on every dashboard refresh. Resolving the predicate against a shard's own min/max instead
 * collapses it to a stable marker on every shard whose data the window provably covers or provably excludes, which is
 * every rolled-over shard the admission policy is interested in.
 */
record LiftedTimeRange(String fieldName, @Nullable Long from, boolean includeFrom, @Nullable Long to, boolean includeTo) {

    void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalLong(from);
        out.writeBoolean(includeFrom);
        out.writeOptionalLong(to);
        out.writeBoolean(includeTo);
    }

    /**
     * Resolves this predicate against one shard's reader.
     *
     * @param context a throwaway copy of the shard's search execution context. {@code isFieldWithinQuery} records the
     *                parsed lower bound on the context it is handed; the copy prevents that from mutating or poisoning
     *                the cacheability flag of the context the query itself is planned against. One copy may be shared
     *                across multiple calls for different fields on the same shard.
     * @return the relation, or {@code null} when it cannot be resolved on this shard, in which case the shard must not
     *         use the cache: the predicate is already out of the fingerprint, so an unresolved relation is not a
     *         missing optimization but a missing part of the key.
     */
    @Nullable
    static Relation resolve(LiftedTimeRange range, SearchExecutionContext context, SearchContext searchContext) throws IOException {
        MappedFieldType fieldType = context.getFieldType(range.fieldName());
        if (fieldType instanceof DateFieldMapper.DateFieldType dateFieldType) {
            if (dateFieldType.resolution() != DateFieldMapper.Resolution.MILLISECONDS) {
                // ES|QL datetime literals are epoch millis; a nanosecond-resolution field would reinterpret them.
                return null;
            }
            return dateFieldType.isFieldWithinQuery(
                searchContext.searcher().getIndexReader(),
                range.from(),
                range.to(),
                range.includeFrom(),
                range.includeTo(),
                null,
                null,
                context
            );
        }
        return null;
    }

    /**
     * Appends the per-shard residue of every lifted predicate to the key: a stable marker where the predicate is
     * provably always-true ({@link Relation#WITHIN}) or always-false ({@link Relation#DISJOINT}) on this reader, and
     * the bounds themselves where they genuinely select ({@link Relation#INTERSECTS}).
     *
     * @return false when any predicate could not be resolved, meaning this shard must not use the cache
     */
    static boolean writeResidue(List<LiftedTimeRange> ranges, SearchContext searchContext, StreamOutput out) throws IOException {
        if (ranges.isEmpty()) {
            return true;
        }
        /*
         * A single throwaway copy is enough for all ranges on this shard: each range is for a different field, so
         * isFieldWithinQuery's per-field state recording does not cross between them.
         */
        SearchExecutionContext throwawayContext = new SearchExecutionContext(searchContext.getSearchExecutionContext());
        for (LiftedTimeRange range : ranges) {
            Relation relation = resolve(range, throwawayContext, searchContext);
            if (relation == null) {
                return false;
            }
            out.writeString(range.fieldName());
            out.writeByte((byte) relation.ordinal());
            if (relation == Relation.INTERSECTS) {
                range.writeTo(out);
            }
        }
        return true;
    }
}
