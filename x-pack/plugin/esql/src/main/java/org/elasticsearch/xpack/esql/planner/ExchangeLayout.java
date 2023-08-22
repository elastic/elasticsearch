/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.ql.expression.NameId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * Decorating layout that creates the NameId -> Value lazily based on the calls made to its content.
 * Essentially it maps the existing (old) NameIds to the new ones.
 */
class ExchangeLayout extends Layout {

    private final Map<NameId, Integer> delegate;
    private final Map<Integer, Set<NameId>> inverse;
    private final Map<NameId, NameId> mappingToOldLayout;
    private int counter;

    ExchangeLayout(Layout layout) {
        super(emptyMap(), 0);
        this.delegate = layout.internalLayout();
        this.mappingToOldLayout = Maps.newMapWithExpectedSize(delegate.size());
        this.inverse = Maps.newMapWithExpectedSize(delegate.size());

        for (Map.Entry<NameId, Integer> entry : delegate.entrySet()) {
            NameId key = entry.getKey();
            Integer value = entry.getValue();
            inverse.computeIfAbsent(value, k -> new HashSet<>()).add(key);
        }
    }

    @Override
    public Integer getChannel(NameId id) {
        var oldId = mappingToOldLayout.get(id);
        if (oldId == null && counter < delegate.size()) {
            var names = inverse.get(counter++);
            for (var name : names) {
                oldId = name;
                mappingToOldLayout.put(id, oldId);
            }
        }
        return delegate.get(oldId);
    }

    @Override
    public int numberOfIds() {
        return delegate.size();
    }

    @Override
    public int numberOfChannels() {
        return inverse.size();
    }
}
