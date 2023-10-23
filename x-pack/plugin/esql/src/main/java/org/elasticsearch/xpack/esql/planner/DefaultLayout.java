/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.ql.expression.NameId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
    public List<ChannelSet> inverse() {
        List<ChannelSet> inverse = new ArrayList<>(numberOfChannels);
        for (int i = 0; i < numberOfChannels; i++) {
            inverse.add(null);
        }
        for (Map.Entry<NameId, ChannelAndType> entry : layout.entrySet()) {
            ChannelSet set = inverse.get(entry.getValue().channel());
            if (set == null) {
                set = new ChannelSet(new HashSet<>(), entry.getValue().type());
                inverse.set(entry.getValue().channel(), set);
            } else {
                if (set.type() != entry.getValue().type()) {
                    throw new IllegalArgumentException();
                }
            }
            set.nameIds().add(entry.getKey());
        }
        return inverse;
    }

    /**
     * @return creates a builder to append to this layout.
     */
    @Override
    public Layout.Builder builder() {
        return new Builder(inverse());
    }

    @Override
    public String toString() {
        return "Layout{layout=" + layout + ", numberOfChannels=" + numberOfChannels + '}';
    }
}
