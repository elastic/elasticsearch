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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to preview a MachineLearning Datafeed 
 */
public class PreviewDatafeedRequest extends ActionRequest implements ToXContentObject {

    public static final ConstructingObjectParser<PreviewDatafeedRequest, Void> PARSER = new ConstructingObjectParser<>(
        "open_datafeed_request", true, a -> new PreviewDatafeedRequest((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DatafeedConfig.ID);
    }

    public static PreviewDatafeedRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String datafeedId;

    /**
     * Create a new request with the desired datafeedId
     *
     * @param datafeedId unique datafeedId, must not be null
     */
    public PreviewDatafeedRequest(String datafeedId) {
        this.datafeedId = Objects.requireNonNull(datafeedId, "[datafeed_id] must not be null");
    }

    public String getDatafeedId() {
        return datafeedId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeedId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        PreviewDatafeedRequest that = (PreviewDatafeedRequest) other;
        return Objects.equals(datafeedId, that.datafeedId);
    }
}
