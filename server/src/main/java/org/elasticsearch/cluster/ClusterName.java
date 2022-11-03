/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final Logger LOGGER = LogManager.getLogger(ClusterName.class);

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

    // TODO: Assume for now isRemote is OK in ClusterName, instead of inside DiscoveryNode. Consider moving to DiscoveryNode later.
    private final boolean isRemote;

    public ClusterName(StreamInput input) throws IOException {
        this(readValue(input), readIsRemote(input), 0);
    }

    public ClusterName(String value) {
        this(value, false, 0);
    }

    public ClusterName(String value, boolean isRemote) {
        this(value, isRemote, 0);
        assert isRemote : "This constructor should only be used for setting isRemote=true";
    }

    // This private constructor is used by all the public constructors. Can't use ClusterName(String,boolean) because it has an assertion.
    private ClusterName(String value, boolean isRemote, int ignore) {
        this.value = value.intern();
        this.isRemote = isRemote;
    }

    public String value() {
        return this.value;
    }

    public boolean isRemote() {
        return isRemote;
    }

    private static String readValue(StreamInput input) throws IOException {
        String s = input.readString();
        LOGGER.info("Read value: [{}], Version [{}]", s, input.getVersion());
        return s;
    }

    private static boolean readIsRemote(StreamInput input) throws IOException {
        if (input.getVersion().onOrAfter(TcpTransport.UNTRUSTED_REMOTE_CLUSTER_FEATURE_VERSION) == false) {
            LOGGER.info("Read IsRemote: [false], SKIPPED because Version: [{}]", input.getVersion());
            return false;
        } else if (TcpTransport.isUntrustedRemoteClusterEnabled() == false) {
            LOGGER.info("Read IsRemote: [false], SKIPPED because isUntrustedRemoteClusterEnabled [disabled]");
            return false;
        }
        final boolean b = input.readBoolean();
        LOGGER.info("Read IsRemote: [{}], READ because isUntrustedRemoteClusterEnabled [enabled]", b);
        return b;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        LOGGER.info("Write value: [{}], Version [{}]", value, out.getVersion());
        out.writeString(value);
        if (out.getVersion().onOrAfter(TcpTransport.UNTRUSTED_REMOTE_CLUSTER_FEATURE_VERSION) == false) {
            LOGGER.info("Write IsRemote: [{}], SKIPPED because Version: [{}]", out.getVersion(), isRemote);
        } else if (TcpTransport.isUntrustedRemoteClusterEnabled() == false) {
            LOGGER.info("Write IsRemote: [{}], SKIPPED because isUntrustedRemoteClusterEnabled [disabled]", isRemote);
        } else {
            LOGGER.info("Write IsRemote: [{}], WRITTEN because isUntrustedRemoteClusterEnabled [enabled]", isRemote);
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
}
