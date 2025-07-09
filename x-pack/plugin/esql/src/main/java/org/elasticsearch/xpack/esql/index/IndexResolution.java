/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class IndexResolution {

    /**
     * @param index EsIndex encapsulating requested index expression, resolved mappings and index modes from field-caps.
     * @param resolvedIndices Set of concrete indices resolved by field-caps. (This information is not always present in the EsIndex).
     * @param unavailableShards Set of shards that were unavailable during index resolution
     * @param unavailableClusters Remote clusters that could not be contacted during planning
     * @return valid IndexResolution
     */
    public static IndexResolution valid(
        EsIndex index,
        Set<String> resolvedIndices,
        Set<NoShardAvailableActionException> unavailableShards,
        Map<String, FieldCapabilitiesFailure> unavailableClusters
    ) {
        Objects.requireNonNull(index, "index must not be null if it was found");
        Objects.requireNonNull(resolvedIndices, "resolvedIndices must not be null");
        Objects.requireNonNull(unavailableShards, "unavailableShards must not be null");
        Objects.requireNonNull(unavailableClusters, "unavailableClusters must not be null");
        return new IndexResolution(index, null, resolvedIndices, unavailableShards, unavailableClusters);
    }

    /**
     * Use this method only if the set of concrete resolved indices is the same as EsIndex#concreteIndices().
     */
    public static IndexResolution valid(EsIndex index) {
        return valid(index, index.concreteIndices(), Set.of(), Map.of());
    }

    public static IndexResolution invalid(String invalid) {
        Objects.requireNonNull(invalid, "invalid must not be null to signal that the index is invalid");
        return new IndexResolution(null, invalid, Set.of(), Set.of(), Map.of());
    }

    public static IndexResolution notFound(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return invalid("Unknown index [" + name + "]");
    }

    private final EsIndex index;
    @Nullable
    private final String invalid;

    // all indices found by field-caps
    private final Set<String> resolvedIndices;
    private final Set<NoShardAvailableActionException> unavailableShards;
    // remote clusters included in the user's index expression that could not be connected to
    private final Map<String, FieldCapabilitiesFailure> unavailableClusters;

    private IndexResolution(
        EsIndex index,
        @Nullable String invalid,
        Set<String> resolvedIndices,
        Set<NoShardAvailableActionException> unavailableShards,
        Map<String, FieldCapabilitiesFailure> unavailableClusters
    ) {
        this.index = index;
        this.invalid = invalid;
        this.resolvedIndices = resolvedIndices;
        this.unavailableShards = unavailableShards;
        this.unavailableClusters = unavailableClusters;
    }

    public boolean matches(String indexName) {
        return isValid() && this.index.name().equals(indexName);
    }

    /**
     * Get the {@linkplain EsIndex}
     * @throws MappingException if the index is invalid for use with ql
     */
    public EsIndex get() {
        if (invalid != null) {
            throw new MappingException(invalid);
        }
        return index;
    }

    /**
     * Is the index valid for use with ql?
     * @return {@code false} if the index wasn't found.
     */
    public boolean isValid() {
        return invalid == null;
    }

    /**
     * @return Map of unavailable clusters (could not be connected to during field-caps query). Key of map is cluster alias,
     * value is the {@link FieldCapabilitiesFailure} describing the issue.
     */
    public Map<String, FieldCapabilitiesFailure> unavailableClusters() {
        return unavailableClusters;
    }

    /**
     * @return all indices found by field-caps (regardless of whether they had any mappings)
     */
    public Set<String> resolvedIndices() {
        return resolvedIndices;
    }

    /**
     * @return set of unavailable shards during index resolution
     */
    public Set<NoShardAvailableActionException> getUnavailableShards() {
        return unavailableShards;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        IndexResolution other = (IndexResolution) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(invalid, other.invalid)
            && Objects.equals(resolvedIndices, other.resolvedIndices)
            && Objects.equals(unavailableClusters, other.unavailableClusters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, invalid, resolvedIndices, unavailableClusters);
    }

    @Override
    public String toString() {
        return invalid != null
            ? invalid
            : "IndexResolution{"
                + "index="
                + index
                + ", invalid='"
                + invalid
                + '\''
                + ", resolvedIndices="
                + resolvedIndices
                + ", unavailableClusters="
                + unavailableClusters
                + '}';
    }
}
