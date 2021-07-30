/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Influence field name and list of influence field values/score pairs
 */
public class Influence implements ToXContentObject {

    /**
     * Note all X-Content serialized field names are "influencer" not "influence"
     */
    public static final ParseField INFLUENCER = new ParseField("influencer");
    public static final ParseField INFLUENCER_FIELD_NAME = new ParseField("influencer_field_name");
    public static final ParseField INFLUENCER_FIELD_VALUES = new ParseField("influencer_field_values");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Influence, Void> PARSER =
        new ConstructingObjectParser<>(INFLUENCER.getPreferredName(), true, a -> new Influence((String) a[0], (List<String>) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_NAME);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_VALUES);
    }

    private String field;
    private List<String> fieldValues;

    Influence(String field, List<String> fieldValues) {
        this.field = field;
        this.fieldValues = Collections.unmodifiableList(fieldValues);
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
