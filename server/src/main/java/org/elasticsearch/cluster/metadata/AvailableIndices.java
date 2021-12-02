/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public class AvailableIndices {

    private final Metadata metadata;
    private final Predicate<IndexAbstraction> predicate;
    private Set<String> availableNames;

    public AvailableIndices(Metadata metadata, Predicate<IndexAbstraction> predicate) {
        this.metadata = Objects.requireNonNull(metadata);
        this.predicate = Objects.requireNonNull(predicate);
    }

    public AvailableIndices(Set<String> availableNames) {
        this.availableNames = availableNames;
        this.metadata = null;
        this.predicate = null;
    }

    public Set<String> getAvailableNames() {
        if (availableNames == null) {
            availableNames = new HashSet<>();
            assert predicate != null && metadata != null;
            for (IndexAbstraction indexAbstraction : metadata.getIndicesLookup().values()) {
                if (predicate.test(indexAbstraction)) {
                    availableNames.add(indexAbstraction.getName());
                }
            }
        }
        return availableNames;
    }

    public boolean isAvailableName(String name) {
        if (availableNames == null) {
            assert predicate != null && metadata != null;
            return predicate.test(metadata.getIndicesLookup().get(name));
        } else {
            return availableNames.contains(name);
        }
    }
}
