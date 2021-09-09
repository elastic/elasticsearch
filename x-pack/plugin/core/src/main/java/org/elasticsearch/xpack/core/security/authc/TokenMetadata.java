/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class TokenMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    /**
     * The type of {@link ClusterState} data.
     */
    public static final String TYPE = "security_tokens";

    private final List<KeyAndTimestamp> keys;

    public List<KeyAndTimestamp> getKeys() {
        return keys;
    }

    private final byte[] currentKeyHash;

    public byte[] getCurrentKeyHash() {
        return currentKeyHash;
    }

    public TokenMetadata(List<KeyAndTimestamp> keys, byte[] currentKeyHash) {
        this.keys = keys;
        this.currentKeyHash = currentKeyHash;
    }

    public TokenMetadata(StreamInput input) throws IOException {
        currentKeyHash = input.readByteArray();
        keys = Collections.unmodifiableList(input.readList(KeyAndTimestamp::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(currentKeyHash);
        out.writeList(keys);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // never render this to the user
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TokenMetadata that = (TokenMetadata)o;
        return keys.equals(that.keys) && Arrays.equals(currentKeyHash, that.currentKeyHash);
    }

    @Override
    public int hashCode() {
        int result = keys.hashCode();
        result = 31 * result + Arrays.hashCode(currentKeyHash);
        return result;
    }

    @Override
    public String toString() {
        return "TokenMetadata{ everything is secret }";
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumIndexCompatibilityVersion();
    }

    @Override
    public boolean isPrivate() {
        // never sent this to a client
        return true;
    }
}
