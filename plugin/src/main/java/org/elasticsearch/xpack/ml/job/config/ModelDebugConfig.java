/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ModelDebugConfig extends ToXContentToBytes implements Writeable {

    private static final ParseField TYPE_FIELD = new ParseField("model_debug_config");
    private static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField TERMS_FIELD = new ParseField("terms");

    public static final ConstructingObjectParser<ModelDebugConfig, Void> PARSER = new ConstructingObjectParser<>(
            TYPE_FIELD.getPreferredName(), a -> new ModelDebugConfig((boolean) a[0], (String) a[1]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TERMS_FIELD);
    }

    private final boolean enabled;
    private final String terms;

    public ModelDebugConfig() {
        this(true, null);
    }

    public ModelDebugConfig(boolean enabled) {
        this(false, null);
    }

    public ModelDebugConfig(boolean enabled, String terms) {
        this.enabled = enabled;
        this.terms = terms;
    }

    public ModelDebugConfig(StreamInput in) throws IOException {
        enabled = in.readBoolean();
        terms = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeOptionalString(terms);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        if (terms != null) {
            builder.field(TERMS_FIELD.getPreferredName(), terms);
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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof ModelDebugConfig == false) {
            return false;
        }

        ModelDebugConfig that = (ModelDebugConfig) other;
        return this.enabled == that.enabled && Objects.equals(this.terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, terms);
    }
}
