/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.ql.expression.NameId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class DefaultLayout implements Layout {
    private final Map<NameId, ChannelAndType> layout;
    private final int numberOfChannels;

    DefaultLayout(Map<NameId, ChannelAndType> layout, int numberOfChannels) {
        this.layout = layout;
        this.numberOfChannels = numberOfChannels;
    }

    @Override
    public ChannelAndType get(NameId id) {
        return layout.get(id);
    }

    /**
     * @return the total number of channels in the layout.
     */
    @Override
    public int numberOfChannels() {
        return numberOfChannels;
    }

    @Override
    public Map<Integer, Set<NameId>> inverse() {
        Map<Integer, Set<NameId>> inverse = new HashMap<>();
        for (Map.Entry<NameId, ChannelAndType> entry : layout.entrySet()) {
            NameId key = entry.getKey();
            Integer value = entry.getValue().channel();
            inverse.computeIfAbsent(value, k -> new HashSet<>()).add(key);
        }
        return inverse;
    }

    /**
     * @return creates a builder to append to this layout.
     */
    @Override
    public Layout.Builder builder() {
        return new Builder(numberOfChannels, layout);
    }

    @Override
    public String toString() {
        return "Layout{" + "layout=" + layout + ", numberOfChannels=" + numberOfChannels + '}';
    }
}
