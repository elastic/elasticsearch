/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.hppc.IntArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Resolves schema leaves owned by a {@link FieldMapper#resolvesColumnGroup() group mapper} — a mapper (such as
 * {@link org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper}) whose source value is an object that the ESCF
 * encoder explodes into one dotted leaf per key. No mapper exists at those descendant paths, so the group mapper claims
 * them en masse instead of resolving them leaf-by-leaf.
 *
 * <p>Resolution walks the leaf's dotted ancestors most-specific first and stops at the first one with a
 * {@link Mapper}, yielding one of the three {@link ColumnGroupLookup} outcomes.
 *
 * <p>The walk uses the flattened dotted path rather than the {@link org.elasticsearch.sourcebatch.SourceSchema}
 * parent-pointer tree, because the row path wraps source in {@code DotExpandingXContentParser} and so cannot tell
 * {@code {"flat.k":"v"}} from {@code {"flat":{"k":"v"}}}. A tree walk would diverge: for {@code {"flat.k":"v"}} the
 * leaf's parent is root, so there is no boundary at {@code flat} and the group lookup would miss.
 *
 * <p>Two columns in one group can share a relative key ({@code {"flat":{"a.b":1}}} and {@code {"flat":{"a":{"b":2}}}}
 * both yield {@code a.b}), including within a single document. That is safe here because a group mapper receives all of
 * its columns at once in {@link FieldMapper#mapColumnGroupBatch} and merges them into one output per document, exactly
 * as the row path does. Per-leaf columns get no such treatment — {@code ShardBatchMapper#resolveMappers} falls back when
 * two leaves bound to a per-leaf mapper share a full path.
 */
public final class ColumnGroupResolver {

    private ColumnGroupResolver() {}

    /** The outcome of walking a leaf's dotted ancestors in {@link #findColumnGroup}. */
    public sealed interface ColumnGroupLookup {

        /** Singleton {@link NotOwned} outcome. */
        ColumnGroupLookup NOT_OWNED = new NotOwned();

        /**
         * The nearest mapped ancestor is a group mapper, which owns this leaf.
         *
         * @param ownerPath   the mapper's full dotted path, used as the group key in {@link Builder}
         * @param relativeKey the leaf's path with {@code ownerPath + "."} stripped — for a
         *                    {@link org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper} this is the flattened key
         */
        record Owned(FieldMapper mapper, String ownerPath, String relativeKey) implements ColumnGroupLookup {}

        /**
         * The nearest mapped ancestor is a plain field mapper, i.e. the document nests values beneath a leaf field.
         * The sequential path rejects this as a document parsing error, so callers must fall back to it rather than
         * treat the leaf as unmapped — under a {@code dynamic: false} prefix that would silently drop the value.
         *
         * @param ownerPath the conflicting mapper's full dotted path
         */
        record Conflict(FieldMapper mapper, String ownerPath) implements ColumnGroupLookup {}

        /** No mapped ancestor, so the leaf is resolved on its own as either a mapped or an unmapped field. */
        record NotOwned() implements ColumnGroupLookup {}
    }

    /**
     * One group mapper together with the schema leaves it owns, in schema order. Leaf indexes are stable across chunk
     * slices — {@code EscfBatch#slice} rebuilds the column array positionally — so a resolution computed once per batch
     * is reused for every chunk.
     *
     * @param leafIndexes  indexes into the batch's column array, in schema order
     * @param relativeKeys the relative key for each leaf, parallel to {@code leafIndexes}
     */
    public record ColumnGroupResolution(FieldMapper mapper, int[] leafIndexes, String[] relativeKeys) {}

    static final ColumnGroupResolution[] EMPTY = new ColumnGroupResolution[0];

    /**
     * Classifies {@code leafPath} against the nearest mapped ancestor on its dotted path.
     *
     * <p>Callers handle a leaf that has its own mapper as a normal leaf and never pass it here, so a sibling mapper
     * declared at a group-key path needs no special treatment.
     */
    public static ColumnGroupLookup findColumnGroup(String leafPath, MappingLookup lookup) {
        int dot = leafPath.lastIndexOf('.');
        while (dot > 0) {
            final String ancestorPath = leafPath.substring(0, dot);
            final Mapper ancestor = lookup.getMapper(ancestorPath);
            if (ancestor instanceof FieldMapper fieldMapper) {
                return fieldMapper.resolvesColumnGroup()
                    ? new ColumnGroupLookup.Owned(fieldMapper, ancestorPath, leafPath.substring(dot + 1))
                    : new ColumnGroupLookup.Conflict(fieldMapper, ancestorPath);
            }
            dot = leafPath.lastIndexOf('.', dot - 1);
        }
        return ColumnGroupLookup.NOT_OWNED;
    }

    /**
     * Accumulates schema leaves per group owner, in first-seen order. Use one {@code Builder} per batch resolution;
     * its state is not thread-safe.
     */
    public static final class Builder {

        /**
         * owner path → accumulator. Insertion order is not required for correctness: distinct groups own distinct
         * mappers, whose output field names are disjoint, so their dispatch order does not affect what is emitted.
         * It is kept so the resulting column layout follows first-seen document order, which makes batches easier to
         * read and compare.
         */
        private final LinkedHashMap<String, GroupEntry> groups = new LinkedHashMap<>();

        public void add(ColumnGroupLookup.Owned owned, int leafIndex) {
            groups.computeIfAbsent(owned.ownerPath(), k -> new GroupEntry(owned.mapper())).add(leafIndex, owned.relativeKey());
        }

        public boolean isEmpty() {
            return groups.isEmpty();
        }

        public ColumnGroupResolution[] build() {
            if (groups.isEmpty()) {
                return EMPTY;
            }
            final ColumnGroupResolution[] result = new ColumnGroupResolution[groups.size()];
            int i = 0;
            for (GroupEntry entry : groups.values()) {
                result[i++] = new ColumnGroupResolution(entry.mapper, entry.leafIndexes(), entry.relativeKeys());
            }
            return result;
        }

        private static final class GroupEntry {
            private final FieldMapper mapper;
            private final IntArrayList leafIndexList = new IntArrayList();
            private final List<String> relativeKeyList = new ArrayList<>();

            GroupEntry(FieldMapper mapper) {
                this.mapper = mapper;
            }

            void add(int leafIndex, String relativeKey) {
                leafIndexList.add(leafIndex);
                relativeKeyList.add(relativeKey);
            }

            int[] leafIndexes() {
                final int[] arr = new int[leafIndexList.size()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = leafIndexList.get(i);
                }
                return arr;
            }

            String[] relativeKeys() {
                return relativeKeyList.toArray(String[]::new);
            }
        }
    }
}
