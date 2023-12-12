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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents an advanced snippet used in filtering processes, providing detailed criteria or rules.
 * This class includes timestamps for the creation and last update of the snippet, along with the
 * actual snippet content represented as a map.
 */
public class FilteringAdvancedSnippet implements Writeable, ToXContentObject {

    private final Instant advancedSnippetCreatedAt;
    private final Instant advancedSnippetUpdatedAt;
    private final Map<String, Object> advancedSnippetValue;

    /**
     * @param advancedSnippetCreatedAt The creation timestamp of the advanced snippet.
     * @param advancedSnippetUpdatedAt The update timestamp of the advanced snippet.
     * @param advancedSnippetValue     The value of the advanced snippet.
     */
    private FilteringAdvancedSnippet(
        Instant advancedSnippetCreatedAt,
        Instant advancedSnippetUpdatedAt,
        Map<String, Object> advancedSnippetValue
    ) {
        this.advancedSnippetCreatedAt = advancedSnippetCreatedAt;
        this.advancedSnippetUpdatedAt = advancedSnippetUpdatedAt;
        this.advancedSnippetValue = advancedSnippetValue;
    }

    public FilteringAdvancedSnippet(StreamInput in) throws IOException {
        this.advancedSnippetCreatedAt = in.readInstant();
        this.advancedSnippetUpdatedAt = in.readInstant();
        this.advancedSnippetValue = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    private static final ParseField CREATED_AT_FIELD = new ParseField("created_at");
    private static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FilteringAdvancedSnippet, Void> PARSER = new ConstructingObjectParser<>(
        "connector_filtering_advanced_snippet",
        true,
        args -> new Builder().setAdvancedSnippetCreatedAt((Instant) args[0])
            .setAdvancedSnippetUpdatedAt((Instant) args[1])
            .setAdvancedSnippetValue((Map<String, Object>) args[2])
            .build()
    );

    static {
        PARSER.declareField(constructorArg(), (p, c) -> Instant.parse(p.text()), CREATED_AT_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), (p, c) -> Instant.parse(p.text()), UPDATED_AT_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), (p, c) -> p.map(), VALUE_FIELD, ObjectParser.ValueType.OBJECT);
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
        out.writeInstant(advancedSnippetCreatedAt);
        out.writeInstant(advancedSnippetUpdatedAt);
        out.writeMap(advancedSnippetValue, StreamOutput::writeString, StreamOutput::writeGenericValue);
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
        private Map<String, Object> advancedSnippetValue;

        public Builder setAdvancedSnippetCreatedAt(Instant advancedSnippetCreatedAt) {
            this.advancedSnippetCreatedAt = advancedSnippetCreatedAt;
            return this;
        }

        public Builder setAdvancedSnippetUpdatedAt(Instant advancedSnippetUpdatedAt) {
            this.advancedSnippetUpdatedAt = advancedSnippetUpdatedAt;
            return this;
        }

        public Builder setAdvancedSnippetValue(Map<String, Object> advancedSnippetValue) {
            this.advancedSnippetValue = advancedSnippetValue;
            return this;
        }

        public FilteringAdvancedSnippet build() {
            return new FilteringAdvancedSnippet(advancedSnippetCreatedAt, advancedSnippetUpdatedAt, advancedSnippetValue);
        }
    }
}
