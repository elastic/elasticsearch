/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rerank;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RerankBuilder implements Writeable, ToXContent {

    public final static ParseField RRF_FIELD = new ParseField("rrf");

    private final static ConstructingObjectParser<RerankBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "rerank",
        args -> new RerankBuilder().rrfBuilder((RRFBuilder) args[0])
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> RRFBuilder.fromXContent(p), RRF_FIELD);
    }

    public static RerankBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (rrfBuilder != null) {
            builder.field(RRF_FIELD.getPreferredName(), rrfBuilder);
        }
        builder.endObject();

        return builder;
    }

    private RRFBuilder rrfBuilder;

    public RerankBuilder() {}

    public RerankBuilder(StreamInput in) throws IOException {
        rrfBuilder = in.readOptionalWriteable(RRFBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(rrfBuilder);
    }

    public RerankBuilder rrfBuilder(RRFBuilder rrfBuilder) {
        this.rrfBuilder = rrfBuilder;
        return this;
    }

    public RRFBuilder rrfBuilder() {
        return rrfBuilder;
    }

    public Reranker reranker(int size) {
        if (rrfBuilder != null) {
            return rrfBuilder().reranker(size);
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RerankBuilder that = (RerankBuilder) o;
        return Objects.equals(rrfBuilder, that.rrfBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rrfBuilder);
    }

    @Override
    public String toString() {
        return toString(EMPTY_PARAMS);
    }

    public String toString(Params params) {
        try {
            return XContentHelper.toXContent(this, XContentType.JSON, params, true).utf8ToString();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }
}
