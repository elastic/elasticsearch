/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.analytics;

import org.elasticsearch.client.ml.inference.results.FeatureImportance;
import org.elasticsearch.client.ml.inference.results.TopClassEntry;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;
import java.util.List;

/**
 * This class parses the superset of all possible fields that may be written by
 * InferenceResults. The warning field is mutually exclusive with all the other fields.
 *
 * In the case of classification results {@link #getValue()} may return a String,
 * Boolean or a Double. For regression results {@link #getValue()} is always
 * a Double.
 */
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ParsedInference extends ParsedAggregation {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ParsedInference, Void> PARSER =
        new ConstructingObjectParser<>(ParsedInference.class.getSimpleName(), true,
            args -> new ParsedInference(args[0], (List<FeatureImportance>) args[1],
                (List<TopClassEntry>) args[2], (String) args[3]));

    public static final ParseField FEATURE_IMPORTANCE = new ParseField("feature_importance");
    public static final ParseField WARNING = new ParseField("warning");
    public static final ParseField TOP_CLASSES = new ParseField("top_classes");

    static {
        PARSER.declareField(optionalConstructorArg(), (p, n) -> {
            Object o;
            XContentParser.Token token = p.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                o = p.text();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                o = p.booleanValue();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                o = p.doubleValue();
            } else {
                throw new XContentParseException(p.getTokenLocation(),
                    "[" + ParsedInference.class.getSimpleName() + "] failed to parse field [" + CommonFields.VALUE + "] "
                        + "value [" + token + "] is not a string, boolean or number");
            }
            return o;
        }, CommonFields.VALUE, ObjectParser.ValueType.VALUE);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> FeatureImportance.fromXContent(p), FEATURE_IMPORTANCE);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> TopClassEntry.fromXContent(p), TOP_CLASSES);
        PARSER.declareString(optionalConstructorArg(), WARNING);
        declareAggregationFields(PARSER);
    }

    public static ParsedInference fromXContent(XContentParser parser, final String name) {
        ParsedInference parsed = PARSER.apply(parser, null);
        parsed.setName(name);
        return parsed;
    }

    private final Object value;
    private final List<FeatureImportance> featureImportance;
    private final List<TopClassEntry> topClasses;
    private final String warning;

    ParsedInference(Object value,
                    List<FeatureImportance> featureImportance,
                    List<TopClassEntry> topClasses,
                    String warning) {
        this.value = value;
        this.warning = warning;
        this.featureImportance = featureImportance;
        this.topClasses = topClasses;
    }

    public Object getValue() {
        return value;
    }

    public List<FeatureImportance> getFeatureImportance() {
        return featureImportance;
    }

    public List<TopClassEntry> getTopClasses() {
        return topClasses;
    }

    public String getWarning() {
        return warning;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (warning != null) {
            builder.field(WARNING.getPreferredName(), warning);
        } else {
            builder.field(CommonFields.VALUE.getPreferredName(), value);
            if (topClasses != null && topClasses.size() > 0) {
                builder.field(TOP_CLASSES.getPreferredName(), topClasses);
            }
            if (featureImportance != null && featureImportance.size() > 0) {
                builder.field(FEATURE_IMPORTANCE.getPreferredName(), featureImportance);
            }
        }
        return builder;
    }

    @Override
    public String getType() {
        return InferencePipelineAggregationBuilder.NAME;
    }
}
