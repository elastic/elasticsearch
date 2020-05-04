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

    private static final int DEFAULT_MAX_PAGE_SEARCH_SIZE = -1;
    private static final float DEFAULT_DOCS_PER_SECOND = -1F;

    private static ConstructingObjectParser<SettingsConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<SettingsConfig, Void> parser = new ConstructingObjectParser<>(
            "transform_config_settings",
            lenient,
            args -> new SettingsConfig((Integer) args[0], (Float) args[1])
        );
        parser.declareIntOrNull(optionalConstructorArg(), DEFAULT_MAX_PAGE_SEARCH_SIZE, TransformField.MAX_PAGE_SEARCH_SIZE);
        parser.declareFloatOrNull(optionalConstructorArg(), DEFAULT_DOCS_PER_SECOND, TransformField.DOCS_PER_SECOND);
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
        // do not write default values
        if (maxPageSearchSize != null && (maxPageSearchSize.equals(DEFAULT_MAX_PAGE_SEARCH_SIZE) == false)) {
            builder.field(TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
        }
        if (docsPerSecond != null && (docsPerSecond.equals(DEFAULT_DOCS_PER_SECOND) == false)) {
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

    public static class Builder {
        private Integer maxPageSearchSize;
        private Float docsPerSecond;

        /**
         * Default builder
         */
        public Builder() {

        }

        /**
         * Builder starting from existing settings as base, for the purpose of partially updating settings.
         *
         * @param base base settings
         */
        public Builder(SettingsConfig base) {
            this.maxPageSearchSize = base.maxPageSearchSize;
            this.docsPerSecond = base.docsPerSecond;
        }

        /**
         * Sets the paging maximum paging maxPageSearchSize that transform can use when
         * pulling the data from the source index.
         *
         * If OOM is triggered, the paging maxPageSearchSize is dynamically reduced so that the transform can continue to gather data.
         *
         * @param maxPageSearchSize Integer value between 10 and 10_000
         * @return the {@link Builder} with the paging maxPageSearchSize set.
         */
        public Builder setMaxPageSearchSize(Integer maxPageSearchSize) {
            this.maxPageSearchSize = maxPageSearchSize == null ? DEFAULT_MAX_PAGE_SEARCH_SIZE : maxPageSearchSize;
            return this;
        }

        /**
         * Sets the docs per second that transform can use when pulling the data from the source index.
         *
         * This setting throttles transform by issuing queries less often, however processing still happens in
         * batches. A value of 0 disables throttling (default).
         *
         * @param docsPerSecond Integer value
         * @return the {@link Builder} with requestsPerSecond set.
         */
        public Builder setRequestsPerSecond(Float docsPerSecond) {
            this.docsPerSecond = docsPerSecond == null ? DEFAULT_DOCS_PER_SECOND : docsPerSecond;
            return this;
        }

        /**
         * Update settings according to given settings config.
         *
         * @param update update settings
         * @return the {@link Builder} with applied updates.
         */
        public Builder update(SettingsConfig update) {
            // if explicit {@code null}s have been set in the update, we do not want to carry the default, but get rid
            // of the setting
            if (update.getDocsPerSecond() != null) {
                this.docsPerSecond = update.getDocsPerSecond().equals(DEFAULT_DOCS_PER_SECOND) ? null : update.getDocsPerSecond();
            }
            if (update.getMaxPageSearchSize() != null) {
                this.maxPageSearchSize = update.getMaxPageSearchSize().equals(DEFAULT_MAX_PAGE_SEARCH_SIZE)
                    ? null
                    : update.getMaxPageSearchSize();
            }

            return this;
        }

        public SettingsConfig build() {
            return new SettingsConfig(maxPageSearchSize, docsPerSecond);
        }
    }
}
