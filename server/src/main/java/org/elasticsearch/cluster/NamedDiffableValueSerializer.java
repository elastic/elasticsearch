/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
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
    public boolean supportsVersion(Diff<T> value, Version version) {
        return version.onOrAfter(((NamedDiff<?>) value).getMinimalSupportedVersion());
    }

    @Override
    public boolean supportsVersion(T value, Version version) {
        return version.onOrAfter(value.getMinimalSupportedVersion());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Diff<T> readDiff(StreamInput in, String key) throws IOException {
        return in.readNamedWriteable(NamedDiff.class, key);
    }
}
