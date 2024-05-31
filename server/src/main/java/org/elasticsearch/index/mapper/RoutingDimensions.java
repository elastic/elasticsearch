/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Tracks dimension fields and uses their values to build a byte array containing a representative hash value.
 */
public class RoutingDimensions implements DocumentDimensions {

    record Dimension(BytesRef name, BytesReference value) {}

    /**
     * A sorted set of the serialized values of dimension fields that will be used
     * for calculating the hash value.
     */
    private final SortedSet<Dimension> dimensions = new TreeSet<>(Comparator.comparing(o -> o.name));

    /**
     * Adds dimension values to routing id calculations.
     */
    @Nullable
    private final IndexRouting.ExtractFromSource.Builder routingBuilder;

    public RoutingDimensions(@Nullable IndexRouting.ExtractFromSource.Builder routingBuilder) {
        this.routingBuilder = routingBuilder;
    }

    final Collection<Dimension> dimensions() {
        return Collections.unmodifiableCollection(dimensions);
    }

    final IndexRouting.ExtractFromSource.Builder routingBuilder() {
        return routingBuilder;
    }

    @Override
    public DocumentDimensions addString(String fieldName, BytesRef utf8Value) {
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
            throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
        }
        return this;
    }

    @Override
    public DocumentDimensions addIp(String fieldName, InetAddress value) {
        return addString(fieldName, NetworkAddress.format(value));
    }

    @Override
    public DocumentDimensions addLong(String fieldName, long value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.write((byte) 'l');
            out.writeLong(value);
            add(fieldName, out.bytes());
        } catch (IOException e) {
            throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
        }
        return this;
    }

    @Override
    public DocumentDimensions addUnsignedLong(String fieldName, long value) {
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
            throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
        }
    }

    @Override
    public DocumentDimensions validate(final IndexSettings settings) {
        return this;
    }

    private void add(String fieldName, BytesReference encoded) throws IOException {
        if (dimensions.add(new Dimension(new BytesRef(fieldName), encoded)) == false) {
            throw new IllegalArgumentException("Dimension field [" + fieldName + "] cannot be a multi-valued field.");
        }
    }
}
