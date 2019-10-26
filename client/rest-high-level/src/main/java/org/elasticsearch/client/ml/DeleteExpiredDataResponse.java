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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;


/**
 * A response acknowledging the deletion of expired data
 */
public class DeleteExpiredDataResponse implements ToXContentObject {

    private static final ParseField DELETED = new ParseField("deleted");

    public DeleteExpiredDataResponse(boolean deleted) {
        this.deleted = deleted;
    }

    public static final ConstructingObjectParser<DeleteExpiredDataResponse, Void> PARSER =
        new ConstructingObjectParser<>("delete_expired_data_response", true,
            a -> new DeleteExpiredDataResponse((Boolean) a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), DELETED);
    }

    public static DeleteExpiredDataResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Boolean deleted;

    public Boolean getDeleted() {
        return deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(deleted);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (deleted != null) {
            builder.field(DELETED.getPreferredName(), deleted);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DeleteExpiredDataResponse response = (DeleteExpiredDataResponse) obj;
        return Objects.equals(deleted, response.deleted);
    }
}
