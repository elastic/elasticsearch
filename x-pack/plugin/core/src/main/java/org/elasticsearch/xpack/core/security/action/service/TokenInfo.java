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

import java.io.IOException;
import java.util.Map;

public class TokenInfo implements Writeable, ToXContentObject {

    private final String name;
    private final TokenSource source;

    private TokenInfo(String name, TokenSource source) {
        this.name = name;
        this.source = source;
    }

    public TokenInfo(StreamInput in) throws IOException {
        this.name = in.readString();
        this.source = in.readEnum(TokenSource.class);
    }

    public String getName() {
        return name;
    }

    public TokenSource getSource() {
        return source;
    }

    public static TokenInfo indexToken(String name) {
        return new TokenInfo(name, TokenSource.INDEX);
    }

    public static TokenInfo fileToken(String name) {
        return new TokenInfo(name, TokenSource.FILE);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(name, Map.of());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeEnum(source);
    }

    public enum TokenSource {
        INDEX, FILE;
    }
}
