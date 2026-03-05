/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SynonymSetSummary implements Writeable, ToXContentObject {

    public static final String NAME_FIELD = "synonyms_set";
    public static final String COUNT_FIELD = "count";
    public static final String TOKEN_COUNT_FIELD = "token_count";

    private static final TransportVersion SYNONYM_SET_TOKEN_COUNT = TransportVersion.fromName("synonym_set_token_count");

    private final String name;
    private final long count;
    private final long tokenCount;

    public SynonymSetSummary(long count, long tokenCount, String name) {
        this.name = name;
        this.count = count;
        this.tokenCount = tokenCount;
    }

    public SynonymSetSummary(StreamInput in) throws IOException {
        this.name = in.readString();
        this.count = in.readVLong();
        if (in.getTransportVersion().supports(SYNONYM_SET_TOKEN_COUNT)) {
            this.tokenCount = in.readVLong();
        } else {
            this.tokenCount = 0;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NAME_FIELD, name);
            builder.field(COUNT_FIELD, count);
            builder.field(TOKEN_COUNT_FIELD, tokenCount);
        }
        builder.endObject();

        return builder;
    }

    public String name() {
        return name;
    }

    public long count() {
        return count;
    }

    public long tokenCount() {
        return tokenCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(count);
        if (out.getTransportVersion().supports(SYNONYM_SET_TOKEN_COUNT)) {
            out.writeVLong(tokenCount);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynonymSetSummary that = (SynonymSetSummary) o;
        return Objects.equals(name, that.name) && Objects.equals(count, that.count) && Objects.equals(tokenCount, that.tokenCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, count, tokenCount);
    }
}
