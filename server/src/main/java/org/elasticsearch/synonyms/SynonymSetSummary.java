/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SynonymSetSummary implements Writeable, ToXContentObject {

    public static final String NAME_FIELD = "synonyms";
    public static final String COUNT_FIELD = "count";

    private final String name;
    private final long count;

    public SynonymSetSummary(long count, String name) {
        this.name = name;
        this.count = count;
    }

    public SynonymSetSummary(StreamInput in) throws IOException {
        this.name = in.readString();
        this.count = in.readVLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NAME_FIELD, name);
            builder.field(COUNT_FIELD, count);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynonymSetSummary that = (SynonymSetSummary) o;
        return Objects.equals(name, that.name) && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, count);
    }
}
