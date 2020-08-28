/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The configuration object for the metrics portion of a rollup job config
 *
 * {
 *     "metrics": [
 *        {
 *            "field": "foo",
 *            "metrics": [ "min", "max", "sum"]
 *        },
 *        {
 *            "field": "bar",
 *            "metrics": [ "max" ]
 *        }
 *     ]
 * }
 */
public class MetricConfig implements Validatable, ToXContentObject {

    static final String NAME = "metrics";
    private static final String FIELD = "field";
    private static final String METRICS = "metrics";

    private static final ConstructingObjectParser<MetricConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, true, args -> {
            @SuppressWarnings("unchecked") List<String> metrics = (List<String>) args[1];
            return new MetricConfig((String) args[0], metrics);
        });
        PARSER.declareString(constructorArg(), new ParseField(FIELD));
        PARSER.declareStringArray(constructorArg(), new ParseField(METRICS));
    }

    private final String field;
    private final List<String> metrics;

    public MetricConfig(final String field, final List<String> metrics) {
        this.field = field;
        this.metrics = metrics;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (field == null || field.isEmpty()) {
            validationException.addValidationError("Field name is required");
        }
        if (metrics == null || metrics.isEmpty()) {
            validationException.addValidationError("Metrics must be a non-null, non-empty array of strings");
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
    }

    /**
     * @return the name of the field used in the metric configuration. Never {@code null}.
     */
    public String getField() {
        return field;
    }

    /**
     * @return the names of the metrics used in the metric configuration. Never {@code null}.
     */
    public List<String> getMetrics() {
        return metrics;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(FIELD, field);
            builder.field(METRICS, metrics);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final MetricConfig that = (MetricConfig) other;
        return Objects.equals(field, that.field) && Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, metrics);
    }

    public static MetricConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
