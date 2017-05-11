/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        return version.onOrAfter(((NamedDiff<?>)value).getMinimalSupportedVersion());
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
