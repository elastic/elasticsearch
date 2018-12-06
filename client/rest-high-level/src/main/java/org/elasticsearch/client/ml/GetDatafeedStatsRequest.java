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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Request object to get {@link org.elasticsearch.client.ml.datafeed.DatafeedStats} by their respective datafeedIds
 *
 * {@code _all} explicitly gets all the datafeeds' statistics in the cluster
 * An empty request (no {@code datafeedId}s) implicitly gets all the datafeeds' statistics in the cluster
 */
public class GetDatafeedStatsRequest extends ActionRequest implements ToXContentObject {

    public static final ParseField ALLOW_NO_DATAFEEDS = new ParseField("allow_no_datafeeds");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetDatafeedStatsRequest, Void> PARSER = new ConstructingObjectParser<>(
        "get_datafeed_stats_request", a -> new GetDatafeedStatsRequest((List<String>) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> Arrays.asList(Strings.commaDelimitedListToStringArray(p.text())),
            DatafeedConfig.ID, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareBoolean(GetDatafeedStatsRequest::setAllowNoDatafeeds, ALLOW_NO_DATAFEEDS);
    }

    private static final String ALL_DATAFEEDS = "_all";

    private final List<String> datafeedIds;
    private Boolean allowNoDatafeeds;

    /**
     * Explicitly gets all datafeeds statistics
     *
     * @return a {@link GetDatafeedStatsRequest} for all existing datafeeds
     */
    public static GetDatafeedStatsRequest getAllDatafeedStatsRequest(){
        return new GetDatafeedStatsRequest(ALL_DATAFEEDS);
    }

    GetDatafeedStatsRequest(List<String> datafeedIds) {
        if (datafeedIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("datafeedIds must not contain null values");
        }
        this.datafeedIds = new ArrayList<>(datafeedIds);
    }

    /**
     * Get the specified Datafeed's statistics via their unique datafeedIds
     *
     * @param datafeedIds must be non-null and each datafeedId must be non-null
     */
    public GetDatafeedStatsRequest(String... datafeedIds) {
        this(Arrays.asList(datafeedIds));
    }

    /**
     * All the datafeedIds for which to get statistics
     */
    public List<String> getDatafeedIds() {
        return datafeedIds;
    }

    public Boolean getAllowNoDatafeeds() {
        return this.allowNoDatafeeds;
    }

    /**
     * Whether to ignore if a wildcard expression matches no datafeeds.
     *
     * This includes {@code _all} string or when no datafeeds have been specified
     *
     * @param allowNoDatafeeds When {@code true} ignore if wildcard or {@code _all} matches no datafeeds. Defaults to {@code true}
     */
    public void setAllowNoDatafeeds(boolean allowNoDatafeeds) {
        this.allowNoDatafeeds = allowNoDatafeeds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeedIds, allowNoDatafeeds);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        GetDatafeedStatsRequest that = (GetDatafeedStatsRequest) other;
        return Objects.equals(datafeedIds, that.datafeedIds) &&
            Objects.equals(allowNoDatafeeds, that.allowNoDatafeeds);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(DatafeedConfig.ID.getPreferredName(), Strings.collectionToCommaDelimitedString(datafeedIds));
        if (allowNoDatafeeds != null) {
            builder.field(ALLOW_NO_DATAFEEDS.getPreferredName(), allowNoDatafeeds);
        }
        builder.endObject();
        return builder;
    }

}
