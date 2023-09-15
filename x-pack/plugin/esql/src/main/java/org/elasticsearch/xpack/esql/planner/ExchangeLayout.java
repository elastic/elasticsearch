/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.ql.expression.NameId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decorating layout that creates the NameId -> Value lazily based on the calls made to its content.
 * Essentially it maps the existing (old) NameIds to the new ones.
 */
class ExchangeLayout implements Layout {
    private final Layout delegate;
    private final List<ChannelSet> inverse;
    private final Map<NameId, NameId> mappingToOldLayout;
    private int counter;

    ExchangeLayout(Layout delegate) {
        this.delegate = delegate;
        this.inverse = delegate.inverse();
        this.mappingToOldLayout = new HashMap<>(inverse.size());
    }

    @Override
    public ChannelAndType get(NameId id) {
        var oldId = mappingToOldLayout.get(id);
        if (oldId == null && counter < inverse.size()) {
            var names = inverse.get(counter++).nameIds();
            for (var name : names) {
                oldId = name;
                mappingToOldLayout.put(id, oldId);
            }
        }
        return delegate.get(oldId);
    }

    @Override
    public int numberOfChannels() {
        return delegate.numberOfChannels();
    }

    @Override
    public String toString() {
        return "ExchangeLayout{" + delegate + '}';
    }

    @Override
    public Builder builder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ChannelSet> inverse() {
        throw new UnsupportedOperationException();
    }
}
