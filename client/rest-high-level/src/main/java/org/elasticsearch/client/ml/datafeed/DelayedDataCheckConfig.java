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
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
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
