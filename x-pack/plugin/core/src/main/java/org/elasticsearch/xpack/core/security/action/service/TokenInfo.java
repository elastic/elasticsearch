/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class TokenInfo implements Writeable, ToXContentObject, Comparable<TokenInfo> {

    private final String name;
    @Nullable
    private final Collection<String> nodeNames;

    private TokenInfo(String name) {
        this(name, null);
    }

    private TokenInfo(String name, Collection<String> nodeNames) {
        this.name = name;
        this.nodeNames = nodeNames;
    }

    public TokenInfo(StreamInput in) throws IOException {
        this.name = in.readString();
        this.nodeNames = in.readOptionalStringList();
    }

    public String getName() {
        return name;
    }

    public TokenSource getSource() {
        return nodeNames == null ? TokenSource.INDEX : TokenSource.FILE;
    }

    public Collection<String> getNodeNames() {
        return nodeNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TokenInfo tokenInfo = (TokenInfo) o;
        return Objects.equals(name, tokenInfo.name) && Objects.equals(nodeNames, tokenInfo.nodeNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, nodeNames);
    }

    @Override
    public String toString() {
        return "TokenInfo{" + "name='" + name + '\'' + ", nodeNames=" + nodeNames + '}';
    }

    public static TokenInfo indexToken(String name) {
        return new TokenInfo(name);
    }

    public static TokenInfo fileToken(String name, Collection<String> nodeNames) {
        return new TokenInfo(name, nodeNames);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (nodeNames == null) {
            return builder.field(name, Map.of());
        } else {
            return builder.field(name, Map.of("nodes", nodeNames));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalStringCollection(nodeNames);
    }

    @Override
    public int compareTo(TokenInfo o) {
        // Not comparing node names since name and source guarantee unique order
        int v = getSource().compareTo(o.getSource());
        if (v == 0) {
            return name.compareTo(o.name);
        } else {
            return v;
        }
    }

    public enum TokenSource {
        INDEX, FILE;
    }
}
