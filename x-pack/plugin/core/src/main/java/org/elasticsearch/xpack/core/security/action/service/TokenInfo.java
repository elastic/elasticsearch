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

public class TokenInfo implements Writeable, ToXContentObject {

    private final String name;
    private final TokenSource source;
    @Nullable
    private final Collection<String> nodeNames;

    private TokenInfo(String name, TokenSource source) {
        this(name, source, null);
    }

    private TokenInfo(String name, TokenSource source, Collection<String> nodeNames) {
        this.name = name;
        this.source = source;
        this.nodeNames = nodeNames;
    }

    public TokenInfo(StreamInput in) throws IOException {
        this.name = in.readString();
        this.source = in.readEnum(TokenSource.class);
        this.nodeNames = in.readOptionalStringList();
    }

    public String getName() {
        return name;
    }

    public TokenSource getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TokenInfo tokenInfo = (TokenInfo) o;
        return Objects.equals(name, tokenInfo.name) && source == tokenInfo.source;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, source);
    }

    public static TokenInfo indexToken(String name) {
        return new TokenInfo(name, TokenSource.INDEX);
    }

    public static TokenInfo fileToken(String name, Collection<String> nodeNames) {
        return new TokenInfo(name, TokenSource.FILE, nodeNames);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (source == TokenSource.INDEX) {
            return builder.field(name, Map.of());
        } else {
            return builder.field(name, Map.of("nodes", nodeNames));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeEnum(source);
        out.writeOptionalStringCollection(nodeNames);
    }

    public enum TokenSource {
        INDEX, FILE;
    }
}
