/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public final class StringToIntSet implements ToIntFunction<SubstringView> {

    private final Set<SubstringView> set;

    public StringToIntSet(Set<String> set) {
        if (set == null || set.isEmpty()) {
            throw new IllegalArgumentException("Set cannot be null or empty");
        }
        this.set = set.stream().map(SubstringView::new).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public int applyAsInt(final SubstringView input) {
        return set.contains(input) ? 1 : -1;
    }
}
