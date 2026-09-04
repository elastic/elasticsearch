/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class IndexResolution {

    public static final IndexResolution EMPTY_SUBQUERY = notFound("empty subquery");

    /**
     * @param index EsIndex encapsulating requested index expression, resolved mappings and index modes from field-caps.
     * @param resolvedIndices Set of concrete indices resolved by field-caps. (This information is not always present in the EsIndex).
     * @param failures failures occurred during field-caps.
     * @return valid IndexResolution
     */
    public static IndexResolution valid(EsIndex index, Set<String> resolvedIndices, Map<String, List<FieldCapabilitiesFailure>> failures) {
        Objects.requireNonNull(index, "index must not be null if it was found");
        Objects.requireNonNull(resolvedIndices, "resolvedIndices must not be null");
        Objects.requireNonNull(failures, "failures must not be null");
        return new IndexResolution(index, null, resolvedIndices, failures);
    }

    /**
     * Use this method only if the set of concrete resolved indices is the same as EsIndex#concreteIndices().
     */
    public static IndexResolution valid(EsIndex index) {
        return valid(index, index.concreteQualifiedIndices(), Map.of());
    }

    public static IndexResolution empty(String indexPattern) {
        return valid(new EsIndex(indexPattern, Map.of(), Map.of(), Map.of(), Map.of()));
    }

    public static IndexResolution invalid(String invalid) {
        Objects.requireNonNull(invalid, "invalid must not be null to signal that the index is invalid");
        return new IndexResolution(null, invalid, Set.of(), Map.of());
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
    // map from cluster alias to failures that occurred during field-caps.
    private final Map<String, List<FieldCapabilitiesFailure>> failures;

    private IndexResolution(
        EsIndex index,
        @Nullable String invalid,
        Set<String> resolvedIndices,
        Map<String, List<FieldCapabilitiesFailure>> failures
    ) {
        this.index = index;
        this.invalid = invalid;
        this.resolvedIndices = resolvedIndices;
        this.failures = failures;
    }

    public boolean matches(String indexName) {
        return isValid() && this.index.name().equals(indexName);
    }

    /**
     * Whether this resolution matched at least one concrete index. A lenient ({@code allowEmpty})
     * lookup that matches nothing returns {@link #empty(String)}, which {@link #isValid()} accepts
     * but which carries no resolved indices; callers deciding "did the lookup find anything?" must
     * use this instead of {@link #isValid()}. "Matched" means field-caps reported at least one
     * concrete index after applying index filters and options: an index that exists but was pruned
     * by an index filter does not count, while a matched index whose mapping is empty does.
     */
    public boolean matchedAnyIndex() {
        return isValid() && resolvedIndices.isEmpty() == false;
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
     * Whether this resolution is valid for use with QL. A valid resolution can represent a lenient lookup that matched no indices,
     * as returned by {@link #empty(String)}; use {@link #matchedAnyIndex()} to distinguish that case.
     * @return {@code false} if the resolution is invalid
     */
    public boolean isValid() {
        return invalid == null;
    }

    /**
     * @return Map from cluster alias to failures that occurred during field-caps.
     */
    public Map<String, List<FieldCapabilitiesFailure>> failures() {
        return failures;
    }

    /**
     * @return all indices found by field-caps (regardless of whether they had any mappings)
     */
    public Set<String> resolvedIndices() {
        return resolvedIndices;
    }

    public boolean includesRemoteIndices() {
        if (isValid()) {
            return resolvedIndices.stream().anyMatch(RemoteClusterAware::isRemoteIndexName);
        }
        return false;
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
            && Objects.equals(failures, other.failures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, invalid, resolvedIndices, failures);
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
                + failures
                + '}';
    }
}
