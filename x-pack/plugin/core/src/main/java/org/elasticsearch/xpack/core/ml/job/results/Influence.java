/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Influence field name and list of influence field values/score pairs
 */
public class Influence implements ToXContentObject, Writeable {

    /**
     * Note all publicly exposed field names are "influencer" not "influence"
     */
    public static final ParseField INFLUENCER = new ParseField("influencer");
    public static final ParseField INFLUENCER_FIELD_NAME = new ParseField("influencer_field_name");
    public static final ParseField INFLUENCER_FIELD_VALUES = new ParseField("influencer_field_values");

    public static final ConstructingObjectParser<Influence, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<Influence, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings({ "unchecked" })
    private static ConstructingObjectParser<Influence, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Influence, Void> parser = new ConstructingObjectParser<>(
            INFLUENCER.getPreferredName(),
            ignoreUnknownFields,
            a -> new Influence((String) a[0], (List<String>) a[1])
        );

        parser.declareString(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_NAME);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_VALUES);

        return parser;
    }

    private String field;
    private List<String> fieldValues;

    public Influence(String field, List<String> fieldValues) {
        this.field = field;
        this.fieldValues = fieldValues;
    }

    public Influence(StreamInput in) throws IOException {
        this.field = in.readString();
        this.fieldValues = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeStringCollection(fieldValues);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INFLUENCER_FIELD_NAME.getPreferredName(), field);
        builder.field(INFLUENCER_FIELD_VALUES.getPreferredName(), fieldValues);
        builder.endObject();
        return builder;
    }

    public String getInfluencerFieldName() {
        return field;
    }

    public List<String> getInfluencerFieldValues() {
        return fieldValues;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, fieldValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Influence other = (Influence) obj;
        return Objects.equals(field, other.field) && Objects.equals(fieldValues, other.fieldValues);
    }
}
