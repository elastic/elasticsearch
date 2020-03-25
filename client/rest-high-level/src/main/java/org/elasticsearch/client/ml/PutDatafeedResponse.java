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

import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response containing the newly created {@link DatafeedConfig}
 */
public class PutDatafeedResponse implements ToXContentObject {

    private DatafeedConfig datafeed;

    public static PutDatafeedResponse fromXContent(XContentParser parser) throws IOException {
        return new PutDatafeedResponse(DatafeedConfig.PARSER.parse(parser, null).build());
    }

    PutDatafeedResponse(DatafeedConfig datafeed) {
        this.datafeed = datafeed;
    }

    public DatafeedConfig getResponse() {
        return datafeed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        datafeed.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        PutDatafeedResponse response = (PutDatafeedResponse) object;
        return Objects.equals(datafeed, response.datafeed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeed);
    }
}
