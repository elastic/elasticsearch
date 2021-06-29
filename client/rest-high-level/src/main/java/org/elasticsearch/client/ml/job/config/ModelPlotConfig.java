/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.ParseField;
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
