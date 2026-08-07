/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Resolves schema leaves that are owned by a {@link FieldMapper#resolvesColumnGroup() group mapper} — a
 * mapper (such as {@link org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper}) whose source value
 * is an object that the ESCF encoder explodes into one dotted leaf per key. No mapper exists at those
 * descendant paths, so they cannot be resolved leaf-by-leaf; instead the group mapper claims them en masse.
 *
 * <h2>Resolution algorithm</h2>
 * <p>
 * Resolution walks the dotted ancestors of a leaf path outward-in (most-specific first). The first ancestor
 * that has a {@link Mapper} terminates the walk:
 * <ul>
 *     <li>if it is a {@link FieldMapper} with {@link FieldMapper#resolvesColumnGroup()} {@code == true},
 *         the leaf belongs to that group;</li>
 *     <li>otherwise (a non-group {@link FieldMapper} or an {@link ObjectMapper}) the leaf is not part of
 *         any group.</li>
 * </ul>
 *
 * <h2>Dotted paths, not schema parent pointers</h2>
 * <p>
 * Resolution uses the flattened dotted-string path from {@code schema.getFullPath(leaf)} rather than
 * walking the {@link org.elasticsearch.sourcebatch.SourceSchema} parent-pointer tree. This is deliberate:
 * the row path wraps source in {@code DotExpandingXContentParser}, so {@code {"flat.k":"v"}} and
 * {@code {"flat":{"k":"v"}}} are indistinguishable by the time mapper lookup happens. Using the dotted
 * string reproduces that collapse; a tree-pointer walk would diverge (for {@code {"flat.k":"v"}} the
 * leaf's parent is root, so there is no boundary at {@code flat}, and the group lookup would miss).
 *
 * <h2>Duplicate relative keys are benign</h2>
 * <p>
 * {@code {"flat":{"a.b":1}}} and {@code {"flat":{"a":{"b":2}}}} in one batch each produce relative key
 * {@code a.b} via the dotted-path walk. Within a given document these columns are absent where the other
 * is present, so emitted slots match the row path exactly. Two columns with the same relative key in one
 * group resolution is expected and is not a correctness problem.
 */
public final class ColumnGroupResolver {

    private ColumnGroupResolver() {}

    /**
     * The group mapper that owns a schema leaf, its own dotted path, and the leaf's path relative to it.
     *
     * @param mapper     the group-owning {@link FieldMapper}
     * @param ownerPath  the mapper's full dotted path, used as the group key in {@link Builder}
     * @param relativeKey the leaf's path with {@code ownerPath + "."} stripped — for a
     *                   {@link org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper} this is
     *                   exactly the flattened key
     */
    public record ColumnGroupMatch(FieldMapper mapper, String ownerPath, String relativeKey) {}

    /**
     * One group mapper together with the schema leaves it owns, in schema order. Leaf indexes are stable
     * across chunk slices — {@code EscfBatch#slice} rebuilds the column array positionally — so a
     * resolution computed once per batch is reused for every chunk.
     *
     * @param mapper      the group-owning {@link FieldMapper}
     * @param leafIndexes indexes into the batch's column array, in schema order
     * @param relativeKeys the relative key for each leaf, parallel to {@code leafIndexes}
     */
    public record ColumnGroupResolution(FieldMapper mapper, int[] leafIndexes, String[] relativeKeys) {}

    /** Shared empty array returned by {@link Builder#build()} when no groups were accumulated. */
    static final ColumnGroupResolution[] EMPTY = new ColumnGroupResolution[0];

    /**
     * Walks the dotted ancestors of {@code leafPath} outward-in (most-specific first). The first ancestor
     * that has a mapper terminates the walk. Returns a match if and only if that ancestor is a
     * {@link FieldMapper} with {@link FieldMapper#resolvesColumnGroup()} {@code == true}; otherwise
     * returns {@code null}.
     *
     * <p>A mapper sitting at a path that coincides with one of a group's keys (a sibling mapper declared
     * at a group-key path) is handled as a normal leaf by the caller and is never passed here, so this
     * method need not handle that case.
     *
     * @param leafPath the full dotted path of the schema leaf
     * @param lookup   the current mapping lookup
     * @return the match, or {@code null} if no group owner was found
     */
    @Nullable
    public static ColumnGroupMatch findColumnGroup(String leafPath, MappingLookup lookup) {
        int dot = leafPath.lastIndexOf('.');
        while (dot > 0) {
            final String ancestorPath = leafPath.substring(0, dot);
            final Mapper ancestor = lookup.getMapper(ancestorPath);
            if (ancestor instanceof FieldMapper fieldMapper) {
                return fieldMapper.resolvesColumnGroup()
                    ? new ColumnGroupMatch(fieldMapper, ancestorPath, leafPath.substring(dot + 1))
                    : null;
            }
            dot = leafPath.lastIndexOf('.', dot - 1);
        }
        return null;
    }

    /**
     * Accumulates schema leaves per group owner, in insertion order (first-seen leaf order). Use one
     * {@code Builder} per batch resolution; its state is not thread-safe.
     */
    public static final class Builder {

        /** owner path → accumulator, insertion-ordered so group dispatch order is deterministic */
        private final LinkedHashMap<String, GroupEntry> groups = new LinkedHashMap<>();

        /**
         * Records that schema leaf at {@code leafIndex} belongs to {@code match}'s group.
         */
        public void add(ColumnGroupMatch match, int leafIndex) {
            groups.computeIfAbsent(match.ownerPath(), k -> new GroupEntry(match.mapper())).add(leafIndex, match.relativeKey());
        }

        /** Returns {@code true} if no leaves have been added. */
        public boolean isEmpty() {
            return groups.isEmpty();
        }

        /**
         * Builds the resolved groups as an array ordered by first-seen leaf index. Returns the shared
         * {@linkplain #EMPTY empty array} when no groups were accumulated.
         */
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
            private final List<Integer> leafIndexList = new ArrayList<>();
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
