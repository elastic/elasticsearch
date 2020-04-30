/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SettingsConfig implements Writeable, ToXContentObject {

    public static final ConstructingObjectParser<SettingsConfig, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<SettingsConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<SettingsConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<SettingsConfig, Void> parser = new ConstructingObjectParser<>(
            "transform_config_settings",
            lenient,
            args -> new SettingsConfig((Integer) args[0], (Float) args[1])
        );
        parser.declareInt(optionalConstructorArg(), TransformField.MAX_PAGE_SEARCH_SIZE);
        parser.declareFloat(optionalConstructorArg(), TransformField.DOCS_PER_SECOND);
        return parser;
    }

    private final Integer maxPageSearchSize;
    private final Float docsPerSecond;

    public SettingsConfig() {
        this(null, null);
    }

    public SettingsConfig(Integer maxPageSearchSize, Float docsPerSecond) {
        this.maxPageSearchSize = maxPageSearchSize;
        this.docsPerSecond = docsPerSecond;
    }

    public SettingsConfig(final StreamInput in) throws IOException {
        this.maxPageSearchSize = in.readOptionalInt();
        this.docsPerSecond = in.readOptionalFloat();
    }

    public Integer getMaxPageSearchSize() {
        return maxPageSearchSize;
    }

    public Float getDocsPerSecond() {
        return docsPerSecond;
    }

    public boolean isValid() {
        return true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(maxPageSearchSize);
        out.writeOptionalFloat(docsPerSecond);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxPageSearchSize != null) {
            builder.field(TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
        }
        if (docsPerSecond != null) {
            builder.field(TransformField.DOCS_PER_SECOND.getPreferredName(), docsPerSecond);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        SettingsConfig that = (SettingsConfig) other;
        return Objects.equals(maxPageSearchSize, that.maxPageSearchSize) && Objects.equals(docsPerSecond, that.docsPerSecond);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPageSearchSize, docsPerSecond);
    }

    public static SettingsConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}
