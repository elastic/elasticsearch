/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;

/**
 * Stores a collection of index names separated into "local" and "remote".
 * This allows the resolution and categorization to take place exactly once per-request.
 */
public final class ResolvedIndices {
    private final List<String> local;
    private final List<String> remote;

    public ResolvedIndices(List<String> local, List<String> remote) {
        this.local = Collections.unmodifiableList(local);
        this.remote = Collections.unmodifiableList(remote);
    }

    /**
     * Returns the collection of index names that have been stored as "local" indices.
     * This is a <code>List</code> because order may be important. For example <code>[ "a*" , "-a1" ]</code> is interpreted differently
     * to <code>[ "-a1", "a*" ]</code>. As a consequence, this list <em>may contain duplicates</em>.
     */
    public List<String> getLocal() {
        return local;
    }

    /**
     * Returns the collection of index names that have been stored as "remote" indices.
     */
    public List<String> getRemote() {
        return remote;
    }

    /**
     * @return <code>true</code> if both the {@link #getLocal() local} and {@link #getRemote() remote} index lists are empty.
     */
    public boolean isEmpty() {
        return local.isEmpty() && remote.isEmpty();
    }

    /**
     * @return <code>true</code> if the {@link #getRemote() remote} index lists is empty, and the local index list contains the
     * {@link IndicesAndAliasesResolverField#NO_INDEX_PLACEHOLDER no-index-placeholder} and nothing else.
     */
    public boolean isNoIndicesPlaceholder() {
        return remote.isEmpty() && local.size() == 1 && local.contains(NO_INDEX_PLACEHOLDER);
    }

    public String[] toArray() {
        final String[] array = new String[local.size() + remote.size()];
        int i = 0;
        for (String index : local) {
            array[i++] = index;
        }
        for (String index : remote) {
            array[i++] = index;
        }
        return array;
    }

    /**
     * Builder class for ResolvedIndices that allows for the building of a list of indices
     * without the need to construct new objects and merging them together
     */
    public static class Builder {

        private final List<String> local = new ArrayList<>();
        private final List<String> remote = new ArrayList<>();

        /** add a local index name */
        public void addLocal(String index) {
            local.add(index);
        }

        /** adds the array of local index names */
        public void addLocal(String[] indices) {
            local.addAll(Arrays.asList(indices));
        }

        /** adds the list of local index names */
        public void addLocal(List<String> indices) {
            local.addAll(indices);
        }

        /** adds the list of remote index names */
        public void addRemote(List<String> indices) {
            remote.addAll(indices);
        }

        /** @return <code>true</code> if both the local and remote index lists are empty. */
        public boolean isEmpty() {
            return local.isEmpty() && remote.isEmpty();
        }

        /** @return a immutable ResolvedIndices instance with the local and remote index lists */
        public ResolvedIndices build() {
            return new ResolvedIndices(local, remote);
        }
    }
}
