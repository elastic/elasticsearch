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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds stats about the content of a script
 */
public final class FieldScriptStats implements Writeable, ToXContentFragment {
    private long maxLines = 0;
    private long totalLines = 0;
    private long maxChars = 0;
    private long totalChars = 0;
    private long maxSourceUsages = 0;
    private long totalSourceUsages = 0;
    private long maxDocUsages = 0;
    private long totalDocUsages = 0;

    FieldScriptStats() {}

    FieldScriptStats(StreamInput in) throws IOException {
        this.maxLines = in.readLong();
        this.totalLines = in.readLong();
        this.maxChars = in.readLong();
        this.totalChars = in.readLong();
        this.maxSourceUsages = in.readLong();
        this.totalSourceUsages = in.readLong();
        this.maxDocUsages = in.readLong();
        this.totalDocUsages = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        builder.field("lines_max", maxLines);
        builder.field("lines_total", totalLines);
        builder.field("chars_max", maxChars);
        builder.field("chars_total", totalChars);
        builder.field("source_max", maxSourceUsages);
        builder.field("source_total", totalSourceUsages);
        builder.field("doc_max", maxDocUsages);
        builder.field("doc_total", totalDocUsages);
        return builder;
    }

    void update(int chars, long lines, int sourceUsages, int docUsages, int count) {
        this.maxChars = Math.max(this.maxChars, chars);
        this.totalChars += (long) chars * count;
        this.maxLines = Math.max(this.maxLines, lines);
        this.totalLines += lines * count;
        this.totalSourceUsages += (long) sourceUsages * count;
        this.maxSourceUsages = Math.max(this.maxSourceUsages, sourceUsages);
        this.totalDocUsages += (long) docUsages * count;
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
        FieldScriptStats that = (FieldScriptStats) o;
        return maxLines == that.maxLines
            && totalLines == that.totalLines
            && maxChars == that.maxChars
            && totalChars == that.totalChars
            && maxSourceUsages == that.maxSourceUsages
            && totalSourceUsages == that.totalSourceUsages
            && maxDocUsages == that.maxDocUsages
            && totalDocUsages == that.totalDocUsages;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxLines, totalLines, maxChars, totalChars, maxSourceUsages, totalSourceUsages, maxDocUsages, totalDocUsages);
    }
}
