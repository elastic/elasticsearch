/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class TermCount implements Writeable, ToXContentFragment {

    public static final String TERM_FIELD = "term";
    public static final String DOC_COUNT_FIELD = "doc_count";

    static final ConstructingObjectParser<TermCount, Void> PARSER = new ConstructingObjectParser<>(
        "term_count",
        true,
        a -> { return new TermCount((String) a[0], (long) a[1]); }
    );
    static {
        PARSER.declareString(constructorArg(), new ParseField(TERM_FIELD));
        PARSER.declareLong(constructorArg(), new ParseField(DOC_COUNT_FIELD));
    }

    private final String term;

    private long docCount;

    public TermCount(StreamInput in) throws IOException {
        term = in.readString();
        docCount = in.readLong();
    }

    public TermCount(String term, long count) {
        this.term = term;
        this.docCount = count;
    }

    public String getTerm() {
        return this.term;
    }

    public long getDocCount() {
        return this.docCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(term);
        out.writeLong(docCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TERM_FIELD, getTerm());
        builder.field(DOC_COUNT_FIELD, getDocCount());
        return builder;
    }

    public static TermCount fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TermCount other = (TermCount) o;
        return Objects.equals(getTerm(), other.getTerm()) && Objects.equals(getDocCount(), other.getDocCount());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTerm(), getDocCount());
    }

    void addToDocCount(long extra) {
        docCount += extra;
    }

    @Override
    public String toString() {
        return term + ":" + docCount;
    }

}
