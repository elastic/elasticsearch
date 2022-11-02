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
import org.elasticsearch.transport.TcpTransport;

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
    // TODO: Assume isRemote is OK to go here instead of DiscoveryNode.
    private final boolean isRemote;

    public ClusterName(StreamInput input) throws IOException {
        // TODO Assume for now that readBoolean does not need a version check for bwc with older clusters.
        this(input.readString(), (TcpTransport.isUntrustedRemoteClusterEnabled() && input.readBoolean()), null);
    }

    public ClusterName(String value) {
        this(value, false, null);
    }

    public ClusterName(String value, boolean isRemote) {
        this(value, isRemote, null);
        assert isRemote : "This constructor should only be used for setting isRemote=true";
    }

    private ClusterName(String value, boolean isRemote, Object ignore) {
        this.value = value.intern();
        this.isRemote = isRemote;
    }

    public String value() {
        return this.value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
        // TODO Assume for now that readBoolean does not need a version check for bwc with older clusters.
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            out.writeBoolean(isRemote);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterName that = (ClusterName) o;

        return Objects.equals(value, that.value) && Objects.equals(isRemote, that.isRemote);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value) + Objects.hash(isRemote);
    }

    @Override
    public String toString() {
        return "Cluster [" + value + "] Remote [" + isRemote + "]";
    }

    /**
     * Test if name values are the same. Ignores isRemote.
     * @return True if name values are the same, false if different.
     */
    public Predicate<ClusterName> getEqualityPredicate() {
        return new Predicate<ClusterName>() {
            @Override
            public boolean test(ClusterName o) {
                return Objects.equals(value, o.value);
            }

            @Override
            public String toString() {
                return "local cluster name [" + ClusterName.this.value() + "]";
            }
        };
    }

    public boolean isRemote() {
        return isRemote;
    }
}
