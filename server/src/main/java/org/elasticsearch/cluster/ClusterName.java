/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

    /**
     * Constructs a new {@link ClusterName} instance by reading from a stream input.
     *
     * @param input the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public ClusterName(StreamInput input) throws IOException {
        this(input.readString());
    }

    /**
     * Constructs a new {@link ClusterName} instance with the specified name value.
     * The value is interned to optimize memory usage since cluster names are typically
     * used as part of settings.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ClusterName clusterName = new ClusterName("my-cluster");
     * }</pre>
     *
     * @param value the cluster name value
     * @throws IllegalArgumentException if the value is empty or contains ':'
     */
    public ClusterName(String value) {
        // cluster name string is most likely part of a setting so we can speed things up over outright interning here
        this.value = Settings.internKeyOrValue(value);
    }

    /**
     * Returns the cluster name value.
     *
     * @return the cluster name as a string
     */
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

    /**
     * Returns a predicate that tests for equality with this cluster name.
     * The predicate can be used to filter or validate cluster names against this instance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ClusterName localCluster = new ClusterName("my-cluster");
     * Predicate<ClusterName> equalityCheck = localCluster.getEqualityPredicate();
     * boolean isMatch = equalityCheck.test(otherClusterName);
     * }</pre>
     *
     * @return a predicate that returns {@code true} if the tested cluster name equals this instance
     */
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
