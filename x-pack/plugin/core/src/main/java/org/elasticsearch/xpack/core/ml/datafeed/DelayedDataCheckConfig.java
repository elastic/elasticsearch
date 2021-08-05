/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.util.Objects;

public class DelayedDataCheckConfig implements ToXContentObject, Writeable {

    public static final TimeValue MAX_DELAYED_DATA_WINDOW = TimeValue.timeValueHours(24);
    public static final int MAX_NUMBER_SPANABLE_BUCKETS = 10_000;

    public static final ParseField ENABLED = new ParseField("enabled");
    public static final ParseField CHECK_WINDOW = new ParseField("check_window");
    public static final ParseField CHECK_FREQUENCY = new ParseField("check_frequency");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<DelayedDataCheckConfig, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<DelayedDataCheckConfig, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<DelayedDataCheckConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DelayedDataCheckConfig, Void> parser = new ConstructingObjectParser<>(
            "delayed_data_check_config",
            ignoreUnknownFields,
            a -> new DelayedDataCheckConfig((Boolean) a[0], (TimeValue) a[1], (TimeValue) a[2])
        );

        parser.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED);
        parser.declareString(
            ConstructingObjectParser.optionalConstructorArg(),
            text -> TimeValue.parseTimeValue(text, CHECK_WINDOW.getPreferredName()),
            CHECK_WINDOW
        );
        parser.declareString(
            ConstructingObjectParser.optionalConstructorArg(),
            text -> TimeValue.parseTimeValue(text, CHECK_FREQUENCY.getPreferredName()),
            CHECK_FREQUENCY
        );
        return parser;
    }

    public static DelayedDataCheckConfig defaultDelayedDataCheckConfig() {
        return new DelayedDataCheckConfig(true, null, null);
    }

    public static DelayedDataCheckConfig enabledDelayedDataCheckConfig(TimeValue checkWindow, TimeValue checkInterval) {
        return new DelayedDataCheckConfig(true, checkWindow, checkInterval);
    }

    public static DelayedDataCheckConfig disabledDelayedDataCheckConfig() {
        return new DelayedDataCheckConfig(false, null, null);
    }

    private final boolean enabled;
    private final TimeValue checkWindow;
    private final TimeValue checkFrequency;

    DelayedDataCheckConfig(Boolean enabled, TimeValue checkWindow, TimeValue checkFrequency) {
        this.enabled = enabled;
        if (enabled && checkWindow != null) {
            TimeUtils.checkPositive(checkWindow, CHECK_WINDOW);
            if (checkWindow.compareTo(MAX_DELAYED_DATA_WINDOW) > 0) {
                throw new IllegalArgumentException("check_window [" + checkWindow.getStringRep() + "] must be less than or equal to [24h]");
            }
        }
        if (enabled && checkFrequency != null) {
            TimeUtils.checkPositive(checkFrequency, CHECK_FREQUENCY);
        }
        this.checkWindow = checkWindow;
        this.checkFrequency = checkFrequency;
    }

    public DelayedDataCheckConfig(StreamInput in) throws IOException {
        enabled = in.readBoolean();
        checkWindow = in.readOptionalTimeValue();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            checkFrequency = in.readOptionalTimeValue();
        } else {
            checkFrequency = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeOptionalTimeValue(checkWindow);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeOptionalTimeValue(checkFrequency);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Nullable
    public TimeValue getCheckWindow() {
        return checkWindow;
    }

    @Nullable
    public TimeValue getCheckFrequency() {
        return checkFrequency;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED.getPreferredName(), enabled);
        if (checkWindow != null) {
            builder.field(CHECK_WINDOW.getPreferredName(), checkWindow.getStringRep());
        }
        if (checkFrequency != null) {
            builder.field(CHECK_FREQUENCY.getPreferredName(), checkFrequency.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, checkWindow, checkFrequency);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DelayedDataCheckConfig other = (DelayedDataCheckConfig) obj;
        return Objects.equals(this.enabled, other.enabled)
            && Objects.equals(this.checkWindow, other.checkWindow)
            && Objects.equals(this.checkFrequency, other.checkFrequency);
    }

}
