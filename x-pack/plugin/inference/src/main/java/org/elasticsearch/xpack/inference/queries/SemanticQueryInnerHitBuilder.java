/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.query.InnerHitBuilder.DEFAULT_FROM;
import static org.elasticsearch.index.query.InnerHitBuilder.DEFAULT_SIZE;

public class SemanticQueryInnerHitBuilder implements Writeable, ToXContentObject {
    private static final ObjectParser<SemanticQueryInnerHitBuilder, Void> PARSER = new ObjectParser<>(
        "semantic_query_inner_hits",
        SemanticQueryInnerHitBuilder::new
    );

    static {
        PARSER.declareInt(SemanticQueryInnerHitBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(SemanticQueryInnerHitBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
    }

    private String fieldName;
    private int from = DEFAULT_FROM;
    private int size = DEFAULT_SIZE;

    public SemanticQueryInnerHitBuilder() {
        this.fieldName = null;
    }

    public SemanticQueryInnerHitBuilder(StreamInput in) throws IOException {
        fieldName = in.readOptionalString();
        from = in.readVInt();
        size = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(fieldName);
        out.writeVInt(from);
        out.writeVInt(size);
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public int getFrom() {
        return from;
    }

    public SemanticQueryInnerHitBuilder setFrom(int from) {
        this.from = from;
        return this;
    }

    public int getSize() {
        return size;
    }

    public SemanticQueryInnerHitBuilder setSize(int size) {
        this.size = size;
        return this;
    }

    public InnerHitBuilder toInnerHitBuilder() {
        if (fieldName == null) {
            throw new IllegalStateException("fieldName must have a value");
        }

        return new InnerHitBuilder(fieldName).setFrom(from)
            .setSize(size)
            .setFetchSourceContext(FetchSourceContext.of(true, null, new String[] { SemanticTextField.getEmbeddingsFieldName(fieldName) }));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Don't include name in XContent because it is hard-coded
        builder.startObject();
        if (from != DEFAULT_FROM) {
            builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        }
        if (size != DEFAULT_SIZE) {
            builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        }
        builder.endObject();
        return builder;
    }

    public static SemanticQueryInnerHitBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new SemanticQueryInnerHitBuilder(), null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SemanticQueryInnerHitBuilder that = (SemanticQueryInnerHitBuilder) o;
        return from == that.from && size == that.size && Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, from, size);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
