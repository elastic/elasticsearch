/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The configuration object containing the delayed data check settings.
 *
 * See {@link DelayedDataCheckConfig#enabledDelayedDataCheckConfig(TimeValue)} for creating a new
 * enabled datacheck with the given check_window
 *
 * See {@link DelayedDataCheckConfig#disabledDelayedDataCheckConfig()} for creating a config for disabling
 * delayed data checking.
 */
public class DelayedDataCheckConfig implements ToXContentObject {

    public static final ParseField ENABLED = new ParseField("enabled");
    public static final ParseField CHECK_WINDOW = new ParseField("check_window");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<DelayedDataCheckConfig, Void> PARSER = new ConstructingObjectParser<>(
        "delayed_data_check_config", true, a -> new DelayedDataCheckConfig((Boolean) a[0], (TimeValue) a[1]));
    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED);
        PARSER.declareString(
            ConstructingObjectParser.optionalConstructorArg(),
            text -> TimeValue.parseTimeValue(text, CHECK_WINDOW.getPreferredName()),
            CHECK_WINDOW);
    }

   /**
    * This creates a new DelayedDataCheckConfig that has a check_window of the passed `timeValue`
    *
    * We query the index to the latest finalized bucket from this TimeValue in the past looking to see if any data has been indexed
    * since the data was read with the Datafeed.
    *
    * The window must be larger than the {@link org.elasticsearch.client.ml.job.config.AnalysisConfig#bucketSpan}, less than
    * 24 hours, and span less than 10,000x buckets.
    *
    *
    * @param timeValue The time length in the past from the latest finalized bucket to look for latent data.
    *                  If `null` is provided, the appropriate window is calculated when it is used
    **/
    public static DelayedDataCheckConfig enabledDelayedDataCheckConfig(TimeValue timeValue) {
        return new DelayedDataCheckConfig(true, timeValue);
    }

    /**
     * This creates a new DelayedDataCheckConfig that disables the data check.
     */
    public static DelayedDataCheckConfig disabledDelayedDataCheckConfig() {
        return new DelayedDataCheckConfig(false, null);
    }

    private final boolean enabled;
    private final TimeValue checkWindow;

    DelayedDataCheckConfig(Boolean enabled, TimeValue checkWindow) {
        this.enabled = enabled;
        this.checkWindow = checkWindow;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Nullable
    public TimeValue getCheckWindow() {
        return checkWindow;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED.getPreferredName(), enabled);
        if (checkWindow != null) {
            builder.field(CHECK_WINDOW.getPreferredName(), checkWindow.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, checkWindow);
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
        return Objects.equals(this.enabled, other.enabled) && Objects.equals(this.checkWindow, other.checkWindow);
    }

}
