/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Value Serializer for named diffables
 */
public class NamedDiffableValueSerializer<T extends NamedDiffable<T>> extends DiffableUtils.DiffableValueSerializer<String, T> {

    private final Class<T> tClass;

    public NamedDiffableValueSerializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public T read(StreamInput in, String key) throws IOException {
        return in.readNamedWriteable(tClass, key);
    }

    @Override
    public boolean supportsVersion(Diff<T> value, TransportVersion version) {
        return ((NamedDiff<?>) value).supportsVersion(version);
    }

    @Override
    public boolean supportsVersion(T value, TransportVersion version) {
        return value.supportsVersion(version);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Diff<T> readDiff(StreamInput in, String key) throws IOException {
        return in.readNamedWriteable(NamedDiff.class, key);
    }
}
