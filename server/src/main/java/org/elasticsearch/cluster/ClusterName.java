/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

public class ClusterName implements Writeable {

    public static final Setting<ClusterName> CLUSTER_NAME_SETTING = new Setting<>("cluster.name", "elasticsearch", (s) -> {
        if (s.isEmpty()) {
            throw new IllegalArgumentException("[cluster.name] must not be empty");
        }
        if (s.contains(":")) {
            throw new IllegalArgumentException("[cluster.name] must not contain ':'");
        }
        return new ClusterName(s);
    }, Setting.Property.NodeScope);

    public static final ClusterName DEFAULT = CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);

    private final String value;

    public ClusterName(StreamInput input) throws IOException {
        this(input.readString());
    }

    public ClusterName(String value) {
        this.value = value.intern();
    }

    public String value() {
        return this.value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterName that = (ClusterName) o;

        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "Cluster [" + value + "]";
    }

    public Predicate<ClusterName> getEqualityPredicate() {
        return new Predicate<ClusterName>() {
            @Override
            public boolean test(ClusterName o) {
                return ClusterName.this.equals(o);
            }

            @Override
            public String toString() {
                return "local cluster name [" + ClusterName.this.value() + "]";
            }
        };
    }
}
