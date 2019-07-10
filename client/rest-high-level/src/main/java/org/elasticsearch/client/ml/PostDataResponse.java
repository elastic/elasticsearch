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

import org.elasticsearch.client.ml.job.process.DataCounts;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response object when posting data to a Machine Learning Job
 */
public class PostDataResponse implements ToXContentObject {

    private DataCounts dataCounts;

    public static PostDataResponse fromXContent(XContentParser parser) throws IOException {
        return new PostDataResponse(DataCounts.PARSER.parse(parser, null));
    }

    public PostDataResponse(DataCounts counts) {
        this.dataCounts = counts;
    }

    public DataCounts getDataCounts() {
        return dataCounts;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return dataCounts.toXContent(builder, params);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PostDataResponse other = (PostDataResponse) obj;
        return Objects.equals(dataCounts, other.dataCounts);
    }

}
