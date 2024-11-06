/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Implementation of routing fields, using field matching based on the routing path content.
 */
public class RoutingPathFields implements RoutingFields {

    /**
     * A map of the serialized values of routing fields that will be used
     * for generating the _tsid field. The map will be used by {@link RoutingPathFields}
     * to build the _tsid field for the document.
     */
    private final SortedMap<BytesRef, List<BytesReference>> routingValues = new TreeMap<>();

    /**
     * Builds the routing. Used for building {@code _id}. If null then skipped.
     */
    @Nullable
    private final IndexRouting.ExtractFromSource.Builder routingBuilder;

    public RoutingPathFields(@Nullable IndexRouting.ExtractFromSource.Builder routingBuilder) {
        this.routingBuilder = routingBuilder;
    }

    final SortedMap<BytesRef, List<BytesReference>> routingValues() {
        return Collections.unmodifiableSortedMap(routingValues);
    }

    final IndexRouting.ExtractFromSource.Builder routingBuilder() {
        return routingBuilder;
    }

    @Override
    public RoutingFields addString(String fieldName, BytesRef utf8Value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.write((byte) 's');
            /*
             * Write in utf8 instead of StreamOutput#writeString which is utf-16-ish
             * so it's easier for folks to reason about the space taken up. Mostly
             * it'll be smaller too.
             */
            out.writeBytesRef(utf8Value);
            add(fieldName, out.bytes());

            if (routingBuilder != null) {
                routingBuilder.addMatching(fieldName, utf8Value);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Routing field cannot be serialized.", e);
        }
        return this;
    }

    @Override
    public RoutingFields addIp(String fieldName, InetAddress value) {
        return addString(fieldName, NetworkAddress.format(value));
    }

    @Override
    public RoutingFields addLong(String fieldName, long value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.write((byte) 'l');
            out.writeLong(value);
            add(fieldName, out.bytes());
        } catch (IOException e) {
            throw new IllegalArgumentException("Routing field cannot be serialized.", e);
        }
        return this;
    }

    @Override
    public RoutingFields addUnsignedLong(String fieldName, long value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Object ul = DocValueFormat.UNSIGNED_LONG_SHIFTED.format(value);
            if (ul instanceof Long l) {
                out.write((byte) 'l');
                out.writeLong(l);
            } else {
                out.write((byte) 'u');
                out.writeLong(value);
            }
            add(fieldName, out.bytes());
            return this;
        } catch (IOException e) {
            throw new IllegalArgumentException("Routing field cannot be serialized.", e);
        }
    }

    @Override
    public RoutingFields addBoolean(String fieldName, boolean value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.write((byte) 'b');
            out.write(value ? 't' : 'f');
            add(fieldName, out.bytes());
        } catch (IOException e) {
            throw new IllegalArgumentException("Routing field cannot be serialized.", e);
        }
        return this;
    }

    private void add(String fieldName, BytesReference encoded) throws IOException {
        BytesRef name = new BytesRef(fieldName);
        List<BytesReference> values = routingValues.get(name);
        if (values == null) {
            // optimize for the common case where routing fields are not multi-valued
            routingValues.put(name, List.of(encoded));
        } else {
            if (values.size() == 1) {
                // converts the immutable list that's optimized for the common case of having only one value to a mutable list
                BytesReference previousValue = values.get(0);
                values = new ArrayList<>(4);
                values.add(previousValue);
                routingValues.put(name, values);
            }
            values.add(encoded);
        }
    }

}
