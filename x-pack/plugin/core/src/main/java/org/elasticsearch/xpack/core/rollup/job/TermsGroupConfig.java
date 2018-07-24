/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
public class TermsGroupConfig implements Writeable, ToXContentFragment {
    private static final String NAME = "term_group_config";
    public static final ObjectParser<TermsGroupConfig.Builder, Void> PARSER = new ObjectParser<>(NAME, TermsGroupConfig.Builder::new);

    private static final ParseField FIELDS = new ParseField("fields");
    private static final List<String> FLOAT_TYPES = Arrays.asList("half_float", "float", "double", "scaled_float");
    private static final List<String> NATURAL_TYPES = Arrays.asList("byte", "short", "integer", "long");
    private final String[] fields;

    static {
        PARSER.declareStringArray(TermsGroupConfig.Builder::setFields, FIELDS);
    }

    private TermsGroupConfig(String[] fields) {
        this.fields = fields;
    }

    TermsGroupConfig(StreamInput in) throws IOException {
        fields = in.readStringArray();
    }

    public String[] getFields() {
        return fields;
    }

    /**
     * This returns a set of aggregation builders which represent the configured
     * set of date histograms.  Used by the rollup indexer to iterate over historical data
     */
    public List<CompositeValuesSourceBuilder<?>> toBuilders() {
        if (fields.length == 0) {
            return Collections.emptyList();
        }

        return Arrays.stream(fields).map(f -> {
            TermsValuesSourceBuilder vsBuilder
                    = new TermsValuesSourceBuilder(RollupField.formatIndexerAggName(f, TermsAggregationBuilder.NAME));
            vsBuilder.field(f);
            vsBuilder.missingBucket(true);
            return vsBuilder;
        }).collect(Collectors.toList());
    }

    /**
     * @return A map representing this config object as a RollupCaps aggregation object
     */
    public Map<String, Object> toAggCap() {
        Map<String, Object> map = new HashMap<>(1);
        map.put("agg", TermsAggregationBuilder.NAME);
        return map;
    }

    public Map<String, Object> getMetadata() {
        return Collections.emptyMap();
    }

    public Set<String> getAllFields() {
        return Arrays.stream(fields).collect(Collectors.toSet());
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                 ActionRequestValidationException validationException) {

        Arrays.stream(fields).forEach(field -> {
            Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
            if (fieldCaps != null && fieldCaps.isEmpty() == false) {
                fieldCaps.forEach((key, value) -> {
                    if (key.equals(KeywordFieldMapper.CONTENT_TYPE) || key.equals(TextFieldMapper.CONTENT_TYPE)) {
                        if (value.isAggregatable() == false) {
                            validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                                    "but is not.");
                        }
                    } else if (FLOAT_TYPES.contains(key)) {
                        if (value.isAggregatable() == false) {
                            validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                                    "but is not.");
                        }
                    } else if (NATURAL_TYPES.contains(key)) {
                        if (value.isAggregatable() == false) {
                            validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                                    "but is not.");
                        }
                    } else {
                        validationException.addValidationError("The field referenced by a terms group must be a [numeric] or " +
                                "[keyword/text] type, but found " + fieldCaps.keySet().toString() + " for field [" + field + "]");
                    }
                });
            } else {
                validationException.addValidationError("Could not find a [numeric] or [keyword/text] field with name [" + field
                        + "] in any of the indices matching the index pattern.");
            }
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELDS.getPreferredName(), fields);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(fields);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TermsGroupConfig that = (TermsGroupConfig) other;

        return Arrays.equals(this.fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class Builder {
        private List<String> fields;

        public List<String> getFields() {
            return fields;
        }

        public TermsGroupConfig.Builder setFields(List<String> fields) {
            this.fields = fields;
            return this;
        }

        public TermsGroupConfig build() {
            if (fields == null || fields.isEmpty()) {
                throw new IllegalArgumentException("Parameter [" + FIELDS + "] must have at least one value.");
            }
            return new TermsGroupConfig(fields.toArray(new String[0]));
        }
    }
}
