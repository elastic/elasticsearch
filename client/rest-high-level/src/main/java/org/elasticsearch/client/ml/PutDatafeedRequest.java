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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to create a new Machine Learning Datafeed given a {@link DatafeedConfig} configuration
 */
public class PutDatafeedRequest extends ActionRequest implements ToXContentObject {

    private final DatafeedConfig datafeed;

    /**
     * Construct a new PutDatafeedRequest
     *
     * @param datafeed a {@link DatafeedConfig} configuration to create
     */
    public PutDatafeedRequest(DatafeedConfig datafeed) {
        this.datafeed = datafeed;
    }

    public DatafeedConfig getDatafeed() {
        return datafeed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return datafeed.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PutDatafeedRequest request = (PutDatafeedRequest) object;
        return Objects.equals(datafeed, request.datafeed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeed);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
