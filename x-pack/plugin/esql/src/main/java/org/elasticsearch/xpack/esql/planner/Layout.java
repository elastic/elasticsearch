/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Maintains the mapping from attribute ids to channels (block index).
 *
 * An attribute can only be mapped to exactly one channel but one channel can be mapped to multiple attributes.
 */
public class Layout {

    private final Map<NameId, Integer> layout;
    private final int numberOfChannels;

    private Layout(Map<NameId, Integer> layout, int numberOfChannels) {
        this.layout = layout;
        this.numberOfChannels = numberOfChannels;
    }

    /**
     * @param id the attribute id
     * @return the channel to which the specific attribute id is mapped or `null` if the attribute id does not exist in the layout.
     */
    public Integer getChannel(NameId id) {
        return layout.get(id);
    }

    /**
     * @return the total number of ids in the layout.
     */
    public int numberOfIds() {
        return layout.size();
    }

    /**
     * @return the total number of channels in the layout.
     */
    public int numberOfChannels() {
        return numberOfChannels;
    }

    /**
     * @return creates a builder to append to this layout.
     */
    public Layout.Builder builder() {
        return new Layout.Builder(this);
    }

    @Override
    public String toString() {
        return "BlockLayout{" + "layout=" + layout + ", numberOfChannels=" + numberOfChannels + '}';
    }

    /**
     * Builder class for Layout. The builder ensures that layouts cannot be altered after creation (through references to the underlying
     * map).
     */
    public static class Builder {

        private final List<Set<NameId>> channels;

        public Builder() {
            this.channels = new ArrayList<>();
        }

        private Builder(Layout layout) {
            channels = IntStream.range(0, layout.numberOfChannels).<Set<NameId>>mapToObj(i -> new HashSet<>()).collect(Collectors.toList());
            for (Map.Entry<NameId, Integer> entry : layout.layout.entrySet()) {
                channels.get(entry.getValue()).add(entry.getKey());
            }
        }

        /**
         * Appends a new channel to the layout. The channel is mapped to a single attribute id.
         * @param id the attribute id
         */
        public Builder appendChannel(NameId id) {
            channels.add(Set.of(id));
            return this;
        }

        /**
         * Appends a new channel to the layout. The channel is mapped to one or more attribute ids.
         * @param ids the attribute ids
         */
        public Builder appendChannel(Set<NameId> ids) {
            if (ids.size() < 1) {
                throw new IllegalArgumentException("Channel must be mapped to at least one id.");
            }
            channels.add(ids);
            return this;
        }

        public Builder appendChannels(Collection<? extends NamedExpression> attributes) {
            for (var attribute : attributes) {
                appendChannel(attribute.id());
            }
            return this;
        }

        public Layout build() {
            Map<NameId, Integer> layout = new HashMap<>();
            int numberOfChannels = 0;
            for (Set<NameId> ids : this.channels) {
                int channel = numberOfChannels++;
                for (NameId id : ids) {
                    layout.put(id, channel);
                }
            }
            return new Layout(Collections.unmodifiableMap(layout), numberOfChannels);
        }
    }
}
