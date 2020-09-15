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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Request to stop Machine Learning Datafeeds
 */
public class StopDatafeedRequest implements Validatable, ToXContentObject {

    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField FORCE = new ParseField("force");
    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<StopDatafeedRequest, Void> PARSER = new ConstructingObjectParser<>(
        "stop_datafeed_request",
         a -> new StopDatafeedRequest((List<String>) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> Arrays.asList(Strings.commaDelimitedListToStringArray(p.text())),
            DatafeedConfig.ID, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareString((obj, val) -> obj.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        PARSER.declareBoolean(StopDatafeedRequest::setForce, FORCE);
        PARSER.declareBoolean(StopDatafeedRequest::setAllowNoMatch, ALLOW_NO_MATCH);
    }

    private static final String ALL_DATAFEEDS = "_all";

    private final List<String> datafeedIds;
    private TimeValue timeout;
    private Boolean force;
    private Boolean allowNoMatch;

    /**
     * Explicitly stop all datafeeds
     *
     * @return a {@link StopDatafeedRequest} for all existing datafeeds
     */
    public static StopDatafeedRequest stopAllDatafeedsRequest(){
        return new StopDatafeedRequest(ALL_DATAFEEDS);
    }

    StopDatafeedRequest(List<String> datafeedIds) {
        if (datafeedIds.isEmpty()) {
            throw new InvalidParameterException("datafeedIds must not be empty");
        }
        if (datafeedIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("datafeedIds must not contain null values");
        }
        this.datafeedIds = new ArrayList<>(datafeedIds);
    }

    /**
     * Close the specified Datafeeds via their unique datafeedIds
     *
     * @param datafeedIds must be non-null and non-empty and each datafeedId must be non-null
     */
    public StopDatafeedRequest(String... datafeedIds) {
        this(Arrays.asList(datafeedIds));
    }

    /**
     * All the datafeedIds to be stopped
     */
    public List<String> getDatafeedIds() {
        return datafeedIds;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * How long to wait for the stop request to complete before timing out.
     *
     * @param timeout Default value: 30 minutes
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public Boolean getForce() {
        return force;
    }

    /**
     * Should the stopping be forced.
     *
     * Use to forcefully stop a datafeed
     *
     * @param force When {@code true} forcefully stop the datafeed. Defaults to {@code false}
     */
    public void setForce(boolean force) {
        this.force = force;
    }

    public Boolean getAllowNoMatch() {
        return this.allowNoMatch;
    }

    /**
     * Whether to ignore if a wildcard expression matches no datafeeds.
     *
     * This includes {@code _all} string.
     *
     * @param allowNoMatch When {@code true} ignore if wildcard or {@code _all} matches no datafeeds. Defaults to {@code true}
     */
    public void setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeedIds, timeout, force, allowNoMatch);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        StopDatafeedRequest that = (StopDatafeedRequest) other;
        return Objects.equals(datafeedIds, that.datafeedIds) &&
            Objects.equals(timeout, that.timeout) &&
            Objects.equals(force, that.force) &&
            Objects.equals(allowNoMatch, that.allowNoMatch);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DatafeedConfig.ID.getPreferredName(), Strings.collectionToCommaDelimitedString(datafeedIds));
        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
        }
        if (force != null) {
            builder.field(FORCE.getPreferredName(), force);
        }
        if (allowNoMatch != null) {
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
