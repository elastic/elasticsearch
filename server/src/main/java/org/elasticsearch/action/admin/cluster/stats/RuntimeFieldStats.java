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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class RuntimeFieldStats implements Writeable, ToXContentObject {
    private final String type;
    int count = 0;
    int indexCount = 0;
    final Set<String> scriptLangs;
    long scriptLessCount = 0;
    long shadowedCount = 0;
    private long maxLines = 0;
    private long totalLines = 0;
    private long maxChars = 0;
    private long totalChars = 0;
    private long maxSourceUsages = 0;
    private long totalSourceUsages = 0;
    private long maxDocUsages = 0;
    private long totalDocUsages = 0;

    RuntimeFieldStats(String type) {
        this.type = Objects.requireNonNull(type);
        this.scriptLangs = new HashSet<>();
    }

    public RuntimeFieldStats(StreamInput in) throws IOException {
        this.type = in.readString();
        this.count = in.readInt();
        this.indexCount = in.readInt();
        this.scriptLangs = in.readSet(StreamInput::readString);
        this.scriptLessCount = in.readLong();
        this.shadowedCount = in.readLong();
        this.maxLines = in.readLong();
        this.totalLines = in.readLong();
        this.maxChars = in.readLong();
        this.totalChars = in.readLong();
        this.maxSourceUsages = in.readLong();
        this.totalSourceUsages = in.readLong();
        this.maxDocUsages = in.readLong();
        this.totalDocUsages = in.readLong();
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
        out.writeLong(maxLines);
        out.writeLong(totalLines);
        out.writeLong(maxChars);
        out.writeLong(totalChars);
        out.writeLong(maxSourceUsages);
        out.writeLong(totalSourceUsages);
        out.writeLong(maxDocUsages);
        out.writeLong(totalDocUsages);
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
        builder.field("lines_max", maxLines);
        builder.field("lines_total", totalLines);
        builder.field("chars_max", maxChars);
        builder.field("chars_total", totalChars);
        builder.field("source_max", maxSourceUsages);
        builder.field("source_total", totalSourceUsages);
        builder.field("doc_max", maxDocUsages);
        builder.field("doc_total", totalDocUsages);
        builder.endObject();
        return builder;
    }

    void update(int chars, long lines, int sourceUsages, int docUsages) {
        this.maxChars = Math.max(this.maxChars, chars);
        this.totalChars += chars;
        this.maxLines = Math.max(this.maxLines, lines);
        this.totalLines += lines;
        this.totalSourceUsages += sourceUsages;
        this.maxSourceUsages = Math.max(this.maxSourceUsages, sourceUsages);
        this.totalDocUsages += docUsages;
        this.maxDocUsages = Math.max(this.maxDocUsages, docUsages);
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
        return count == that.count &&
            indexCount == that.indexCount &&
            scriptLessCount == that.scriptLessCount &&
            shadowedCount == that.shadowedCount &&
            maxLines == that.maxLines &&
            totalLines == that.totalLines &&
            maxChars == that.maxChars &&
            totalChars == that.totalChars &&
            maxSourceUsages == that.maxSourceUsages &&
            totalSourceUsages == that.totalSourceUsages &&
            maxDocUsages == that.maxDocUsages &&
            totalDocUsages == that.totalDocUsages &&
            type.equals(that.type) &&
            scriptLangs.equals(that.scriptLangs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, count, indexCount, scriptLangs, scriptLessCount, shadowedCount,
            maxLines, totalLines, maxChars, totalChars,
            maxSourceUsages, totalSourceUsages, maxDocUsages, totalDocUsages);
    }
}
