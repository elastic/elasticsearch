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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents the details of a validation process, including identifiers and descriptive messages.
 * This class is used to encapsulate information about specific validation checks, where each validation
 * is associated with a list of IDs and corresponding messages that detail the validation results.
 */
public class FilteringValidation implements Writeable, ToXContentObject {
    private final List<String> ids;
    private final List<String> messages;

    /**
     * Constructs a new FilteringValidation instance.
     *
     * @param ids      The list of identifiers associated with the validation.
     * @param messages The list of messages describing the validation results.
     */
    public FilteringValidation(List<String> ids, List<String> messages) {
        this.ids = ids;
        this.messages = messages;
    }

    public FilteringValidation(StreamInput in) throws IOException {
        this.ids = in.readStringCollectionAsList();
        this.messages = in.readStringCollectionAsList();
    }

    private static final ParseField IDS_FIELD = new ParseField("ids");
    private static final ParseField MESSAGES_FIELD = new ParseField("messages");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FilteringValidation, Void> PARSER = new ConstructingObjectParser<>(
        "connector_filtering_validation",
        true,
        args -> new Builder().setIds((List<String>) args[0]).setMessages((List<String>) args[1]).build()
    );

    static {
        PARSER.declareStringArray(constructorArg(), IDS_FIELD);
        PARSER.declareStringArray(constructorArg(), MESSAGES_FIELD);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.stringListField(IDS_FIELD.getPreferredName(), ids);
            builder.stringListField(MESSAGES_FIELD.getPreferredName(), messages);
        }
        builder.endObject();
        return builder;
    }

    public static FilteringValidation fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(ids);
        out.writeStringCollection(messages);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilteringValidation that = (FilteringValidation) o;
        return Objects.equals(ids, that.ids) && Objects.equals(messages, that.messages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, messages);
    }

    public static class Builder {

        private List<String> ids;
        private List<String> messages;

        public Builder setIds(List<String> ids) {
            this.ids = ids;
            return this;
        }

        public Builder setMessages(List<String> messages) {
            this.messages = messages;
            return this;
        }

        public FilteringValidation build() {
            return new FilteringValidation(ids, messages);
        }
    }
}
