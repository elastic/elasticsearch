/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public final class SubstringToIntMap implements ToIntFunction<SubstringView> {

    private final Map<SubstringView, Integer> map;

    public SubstringToIntMap(Map<String, Integer> map) {
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException("Map cannot be null or empty");
        }
        this.map = Map.copyOf(
            map.entrySet().stream().collect(Collectors.toMap(entry -> new SubstringView(entry.getKey()), Map.Entry::getValue))
        );
    }

    @Override
    public int applyAsInt(final SubstringView input) {
        Integer value = map.get(input);
        return value != null ? value : -1;
    }
}
