/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The configuration object for the histograms in the rollup config
 *
 * {
 *     "groups": [
 *        "terms": {
 *            "fields" : [ "foo", "bar" ]
 *        }
 *     ]
 * }
 */
public class TermsGroupConfig implements Writeable, ToXContentObject {

    public static final String NAME = "terms";
    private static final String FIELDS = "fields";

    private static final List<String> FLOAT_TYPES = Arrays.asList("half_float", "float", "double", "scaled_float");
    private static final List<String> NATURAL_TYPES = Arrays.asList("byte", "short", "integer", "long");

    private static final ConstructingObjectParser<TermsGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, args -> {
            @SuppressWarnings("unchecked")
            List<String> fields = (List<String>) args[0];
            return new TermsGroupConfig(fields != null ? fields.toArray(new String[fields.size()]) : null);
        });
        PARSER.declareStringArray(constructorArg(), new ParseField(FIELDS));
    }

    private final String[] fields;

    public TermsGroupConfig(final String... fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("Fields must have at least one value");
        }
        this.fields = fields;
    }

    public TermsGroupConfig(StreamInput in) throws IOException {
        fields = in.readStringArray();
    }

    /**
     * @return the names of the fields. Never {@code null}.
     */
    public String[] getFields() {
        return fields;
    }

    public void validateMappings(
        Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
        ActionRequestValidationException validationException
    ) {

        Arrays.stream(fields).forEach(field -> {
            Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
            if (fieldCaps != null && fieldCaps.isEmpty() == false) {
                fieldCaps.forEach((key, value) -> {
                    if (key.equals(KeywordFieldMapper.CONTENT_TYPE) || key.equals(TextFieldMapper.CONTENT_TYPE)) {
                        if (value.isAggregatable() == false) {
                            validationException.addValidationError(
                                "The field [" + field + "] must be aggregatable across all indices, but is not."
                            );
                        }
                    } else if (FLOAT_TYPES.contains(key)) {
                        if (value.isAggregatable() == false) {
                            validationException.addValidationError(
                                "The field [" + field + "] must be aggregatable across all indices, but is not."
                            );
                        }
                    } else if (NATURAL_TYPES.contains(key)) {
                        if (value.isAggregatable() == false) {
                            validationException.addValidationError(
                                "The field [" + field + "] must be aggregatable across all indices, but is not."
                            );
                        }
                    } else {
                        validationException.addValidationError(
                            "The field referenced by a terms group must be a [numeric] or "
                                + "[keyword/text] type, but found "
                                + fieldCaps.keySet().toString()
                                + " for field ["
                                + field
                                + "]"
                        );
                    }
                });
            } else {
                validationException.addValidationError(
                    "Could not find a [numeric] or [keyword/text] field with name ["
                        + field
                        + "] in any of the indices matching the index pattern."
                );
            }
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(FIELDS, fields);
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(fields);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final TermsGroupConfig that = (TermsGroupConfig) other;
        return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static TermsGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
