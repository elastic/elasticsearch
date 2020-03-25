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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response indicating if the Machine Learning Job is now opened or not
 */
public class OpenJobResponse implements ToXContentObject {

    private static final ParseField OPENED = new ParseField("opened");

    public static final ConstructingObjectParser<OpenJobResponse, Void> PARSER =
        new ConstructingObjectParser<>("open_job_response", true, (a) -> new OpenJobResponse((Boolean)a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), OPENED);
    }

    private boolean opened;

    OpenJobResponse(boolean opened) {
        this.opened = opened;
    }

    public static OpenJobResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Has the job opened or not
     *
     * @return boolean value indicating the job opened status
     */
    public boolean isOpened() {
        return opened;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        OpenJobResponse that = (OpenJobResponse) other;
        return isOpened() == that.isOpened();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isOpened());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(OPENED.getPreferredName(), opened);
        builder.endObject();
        return builder;
    }
}
