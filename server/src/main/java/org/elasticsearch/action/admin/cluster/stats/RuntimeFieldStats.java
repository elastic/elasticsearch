/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class RuntimeFieldStats implements Writeable, ToXContentObject {
    private final String type;
    final FieldScriptStats fieldScriptStats;
    int count = 0;
    int indexCount = 0;
    final Set<String> scriptLangs;
    long scriptLessCount = 0;
    long shadowedCount = 0;

    RuntimeFieldStats(String type) {
        this.type = Objects.requireNonNull(type);
        this.scriptLangs = new HashSet<>();
        this.fieldScriptStats = new FieldScriptStats();
    }

    public RuntimeFieldStats(StreamInput in) throws IOException {
        this.type = in.readString();
        this.count = in.readInt();
        this.indexCount = in.readInt();
        this.scriptLangs = in.readSet(StreamInput::readString);
        this.scriptLessCount = in.readLong();
        this.shadowedCount = in.readLong();
        this.fieldScriptStats = new FieldScriptStats(in);
    }

    String type() {
        return type;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeInt(count);
        out.writeInt(indexCount);
        out.writeCollection(scriptLangs, StreamOutput::writeString);
        out.writeLong(scriptLessCount);
        out.writeLong(shadowedCount);
        fieldScriptStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", type);
        builder.field("count", count);
        builder.field("index_count", indexCount);
        builder.field("scriptless_count", scriptLessCount);
        builder.field("shadowed_count", shadowedCount);
        builder.array("lang", scriptLangs.toArray(new String[0]));
        fieldScriptStats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RuntimeFieldStats that = (RuntimeFieldStats) o;
        return count == that.count
            && indexCount == that.indexCount
            && scriptLessCount == that.scriptLessCount
            && shadowedCount == that.shadowedCount
            && fieldScriptStats.equals(that.fieldScriptStats)
            && type.equals(that.type)
            && scriptLangs.equals(that.scriptLangs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, count, indexCount, scriptLangs, scriptLessCount, shadowedCount, fieldScriptStats);
    }
}
