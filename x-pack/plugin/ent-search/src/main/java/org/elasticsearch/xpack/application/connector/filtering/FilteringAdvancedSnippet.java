/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.ConnectorUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents an advanced snippet used in filtering processes, providing detailed criteria or rules.
 * This class includes timestamps for the creation and last update of the snippet, along with the
 * actual snippet content represented as a map.
 */
public class FilteringAdvancedSnippet implements Writeable, ToXContentObject {
    @Nullable
    private final Instant advancedSnippetCreatedAt;
    @Nullable
    private final Instant advancedSnippetUpdatedAt;
    private final Object advancedSnippetValue;

    /**
     * @param advancedSnippetCreatedAt The creation timestamp of the advanced snippet.
     * @param advancedSnippetUpdatedAt The update timestamp of the advanced snippet.
     * @param advancedSnippetValue     The value of the advanced snippet.
     */
    private FilteringAdvancedSnippet(Instant advancedSnippetCreatedAt, Instant advancedSnippetUpdatedAt, Object advancedSnippetValue) {
        this.advancedSnippetCreatedAt = advancedSnippetCreatedAt;
        this.advancedSnippetUpdatedAt = advancedSnippetUpdatedAt;
        this.advancedSnippetValue = advancedSnippetValue;
    }

    public FilteringAdvancedSnippet(StreamInput in) throws IOException {
        this.advancedSnippetCreatedAt = in.readOptionalInstant();
        this.advancedSnippetUpdatedAt = in.readOptionalInstant();
        this.advancedSnippetValue = in.readGenericValue();
    }

    private static final ParseField CREATED_AT_FIELD = new ParseField("created_at");
    private static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    public Instant getAdvancedSnippetCreatedAt() {
        return advancedSnippetCreatedAt;
    }

    public Instant getAdvancedSnippetUpdatedAt() {
        return advancedSnippetUpdatedAt;
    }

    public Object getAdvancedSnippetValue() {
        return advancedSnippetValue;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FilteringAdvancedSnippet, Void> PARSER = new ConstructingObjectParser<>(
        "connector_filtering_advanced_snippet",
        true,
        args -> new Builder().setAdvancedSnippetCreatedAt((Instant) args[0])
            .setAdvancedSnippetUpdatedAt((Instant) args[1])
            .setAdvancedSnippetValue(args[2])
            .build()
    );

    static {
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorUtils.parseInstant(p, CREATED_AT_FIELD.getPreferredName()),
            CREATED_AT_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorUtils.parseInstant(p, UPDATED_AT_FIELD.getPreferredName()),
            UPDATED_AT_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(constructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.START_ARRAY) {
                return p.list();
            } else if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                return p.map();
            }
            throw new XContentParseException("Unsupported token [" + p.currentToken() + "]. Expected an array or an object.");
        }, VALUE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(CREATED_AT_FIELD.getPreferredName(), advancedSnippetCreatedAt);
            builder.field(UPDATED_AT_FIELD.getPreferredName(), advancedSnippetUpdatedAt);
            builder.field(VALUE_FIELD.getPreferredName(), advancedSnippetValue);
        }
        builder.endObject();
        return builder;
    }

    public static FilteringAdvancedSnippet fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInstant(advancedSnippetCreatedAt);
        out.writeOptionalInstant(advancedSnippetUpdatedAt);
        out.writeGenericValue(advancedSnippetValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilteringAdvancedSnippet that = (FilteringAdvancedSnippet) o;
        return Objects.equals(advancedSnippetCreatedAt, that.advancedSnippetCreatedAt)
            && Objects.equals(advancedSnippetUpdatedAt, that.advancedSnippetUpdatedAt)
            && Objects.equals(advancedSnippetValue, that.advancedSnippetValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(advancedSnippetCreatedAt, advancedSnippetUpdatedAt, advancedSnippetValue);
    }

    public static class Builder {

        private Instant advancedSnippetCreatedAt;
        private Instant advancedSnippetUpdatedAt;
        private Object advancedSnippetValue;
        private final Instant currentTimestamp = Instant.now();

        public Builder setAdvancedSnippetCreatedAt(Instant advancedSnippetCreatedAt) {
            this.advancedSnippetCreatedAt = Objects.requireNonNullElse(advancedSnippetCreatedAt, currentTimestamp);
            return this;
        }

        public Builder setAdvancedSnippetUpdatedAt(Instant advancedSnippetUpdatedAt) {
            this.advancedSnippetUpdatedAt = Objects.requireNonNullElse(advancedSnippetUpdatedAt, currentTimestamp);
            return this;
        }

        public Builder setAdvancedSnippetValue(Object advancedSnippetValue) {
            this.advancedSnippetValue = advancedSnippetValue;
            return this;
        }

        public FilteringAdvancedSnippet build() {
            return new FilteringAdvancedSnippet(advancedSnippetCreatedAt, advancedSnippetUpdatedAt, advancedSnippetValue);
        }
    }
}
