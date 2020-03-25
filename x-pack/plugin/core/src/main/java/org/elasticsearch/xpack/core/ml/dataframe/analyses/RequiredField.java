/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class RequiredField {

    private final String name;

    /**
     * The required field must have one of those types.
     * We use a sorted set to ensure types are reported alphabetically in error messages.
     */
    private final SortedSet<String> types;

    public RequiredField(String name, Set<String> types) {
        this.name = Objects.requireNonNull(name);
        this.types = Collections.unmodifiableSortedSet(new TreeSet<>(types));
    }

    public String getName() {
        return name;
    }

    public SortedSet<String> getTypes() {
        return types;
    }
}
