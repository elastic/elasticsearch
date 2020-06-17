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
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ModelPlotConfig implements ToXContentObject {

    private static final ParseField TYPE_FIELD = new ParseField("model_plot_config");
    private static final ParseField ENABLED_FIELD = new ParseField("enabled");
    private static final ParseField TERMS_FIELD = new ParseField("terms");
    private static final ParseField ANNOTATIONS_ENABLED_FIELD = new ParseField("annotations_enabled");

    public static final ConstructingObjectParser<ModelPlotConfig, Void> PARSER =
        new ConstructingObjectParser<>(
            TYPE_FIELD.getPreferredName(), true, a -> new ModelPlotConfig((boolean) a[0], (String) a[1], (Boolean) a[2]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TERMS_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ANNOTATIONS_ENABLED_FIELD);
    }

    private final boolean enabled;
    private final String terms;
    private final Boolean annotationsEnabled;

    public ModelPlotConfig(boolean enabled, String terms, Boolean annotationsEnabled) {
        this.enabled = enabled;
        this.terms = terms;
        this.annotationsEnabled = annotationsEnabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        if (terms != null) {
            builder.field(TERMS_FIELD.getPreferredName(), terms);
        }
        if (annotationsEnabled != null) {
            builder.field(ANNOTATIONS_ENABLED_FIELD.getPreferredName(), annotationsEnabled);
        }
        builder.endObject();
        return builder;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getTerms() {
        return this.terms;
    }

    public Boolean annotationsEnabled() {
        return annotationsEnabled;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof ModelPlotConfig == false) {
            return false;
        }

        ModelPlotConfig that = (ModelPlotConfig) other;
        return this.enabled == that.enabled
            && Objects.equals(this.terms, that.terms)
            && Objects.equals(this.annotationsEnabled, that.annotationsEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, terms, annotationsEnabled);
    }
}
