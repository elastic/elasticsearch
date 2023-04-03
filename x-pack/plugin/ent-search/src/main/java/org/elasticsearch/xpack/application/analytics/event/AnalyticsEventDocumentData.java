/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsEventDocumentData implements ToXContent, Writeable {

    public static ParseField DOCUMENT_FIELD = new ParseField("document");

    public static ParseField DOCUMENT_ID_FIELD = new ParseField("id");

    public static ParseField DOCUMENT_INDEX_FIELD = new ParseField("index");

    private static final ConstructingObjectParser<AnalyticsEventDocumentData, AnalyticsEvent.Context> PARSER =
        new ConstructingObjectParser<>(
            DOCUMENT_FIELD.getPreferredName(),
            false,
            (p, c) -> new AnalyticsEventDocumentData((String) p[0], (String) p[1])
        );

    static {
        PARSER.declareString(
            ConstructingObjectParser.constructorArg(),
            s -> Strings.requireNonBlank(s, "field [id] can't be blank"),
            DOCUMENT_ID_FIELD
        );

        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DOCUMENT_INDEX_FIELD);
    }

    private final String id;

    private final String index;

    public AnalyticsEventDocumentData(String id, @Nullable String index) {
        this.id = Objects.requireNonNull(id);
        this.index = index;
    }

    protected AnalyticsEventDocumentData(String id) {
        this(id, null);
    }

    public AnalyticsEventDocumentData(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString());
    }

    public static AnalyticsEventDocumentData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public String id() {
        return id;
    }

    public String index() {
        return index;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(DOCUMENT_ID_FIELD.getPreferredName(), id());

            if (Objects.nonNull(index)) {
                builder.field(DOCUMENT_INDEX_FIELD.getPreferredName(), index());
            }
        }
        return builder.endObject();
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalString(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventDocumentData that = (AnalyticsEventDocumentData) o;
        return id.equals(that.id) && Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index);
    }
}
