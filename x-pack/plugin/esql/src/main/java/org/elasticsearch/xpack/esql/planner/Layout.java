/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maintains the mapping from attribute ids to channels (block index).
 *
 * An attribute can only be mapped to exactly one channel but one channel can be mapped to multiple attributes.
 */
public interface Layout {
    /**
     * The values stored in the {@link Layout}, a channel id and a {@link DataType}.
     */
    record ChannelAndType(int channel, DataType type) {}

    /**
     * A part of an "inverse" layout, a {@link Set} or {@link NameId}s and a {@link DataType}.
     */
    record ChannelSet(Set<NameId> nameIds, DataType type) {}

    /**
     * @param id the attribute id
     * @return the channel to which the specific attribute id is mapped or `null` if the attribute id does not exist in the layout.
     */
    ChannelAndType get(NameId id);

    /**
     * @return the total number of channels in the layout.
     */
    int numberOfChannels();

    /**
     * @return creates a builder to append to this layout.
     */
    Layout.Builder builder();

    /**
     * Build a list whose index is each channel id and who's values are
     * all link {@link NameId}s at that position and their {@link DataType}.
     */
    List<ChannelSet> inverse();

    /**
     * Builder class for Layout. The builder ensures that layouts cannot be altered after creation (through references to the underlying
     * map).
     */
    class Builder {
        private final List<ChannelSet> channels;

        public Builder() {
            this(new ArrayList<>());
        }

        Builder(List<ChannelSet> channels) {
            this.channels = channels;
        }

        /**
         * Appends a new channel to the layout. The channel is mapped to one or more attribute ids.
         */
        public Builder append(ChannelSet set) {
            if (set.nameIds.isEmpty()) {
                throw new IllegalArgumentException("Channel must be mapped to at least one id.");
            }
            channels.add(set);
            return this;
        }

        /**
         * Appends a new channel to the layout. The channel is mapped to a single attribute id.
         */
        public Builder append(NamedExpression attribute) {
            return append(new ChannelSet(Set.of(attribute.id()), attribute.dataType()));
        }

        /**
         * Appends many new channels to the layout. Each channel is mapped to a single attribute id.
         */
        public Builder append(Collection<? extends NamedExpression> attributes) {
            for (NamedExpression attribute : attributes) {
                append(new ChannelSet(Set.of(attribute.id()), attribute.dataType()));
            }
            return this;
        }

        /**
         * Build a new {@link Layout}.
         */
        public Layout build() {
            int size = 0;
            for (ChannelSet channel : channels) {
                size += channel.nameIds.size();
            }
            Map<NameId, ChannelAndType> layout = Maps.newHashMapWithExpectedSize(size);
            int numberOfChannels = 0;
            for (ChannelSet set : channels) {
                int channel = numberOfChannels++;
                for (NameId id : set.nameIds) {
                    // Duplicate name ids would mean that have 2 channels that are declared under the same id. That makes no sense - which
                    // channel should subsequent operators use, then, when they want to refer to this id?
                    assert (layout.containsKey(id) == false) : "Duplicate name ids are not allowed in layouts";
                    ChannelAndType next = new ChannelAndType(channel, set.type);
                    layout.put(id, next);
                }
            }
            return new DefaultLayout(Collections.unmodifiableMap(layout), numberOfChannels);
        }

        public void replace(NameId id, NameId id1) {
            for (ChannelSet channel : this.channels) {
                if (channel != null && channel.nameIds.contains(id)) {
                    channel.nameIds.remove(id);
                    channel.nameIds.add(id1);
                }
            }
        }
    }
}
