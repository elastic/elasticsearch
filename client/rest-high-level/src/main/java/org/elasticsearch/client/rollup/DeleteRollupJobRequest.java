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
package org.elasticsearch.client.rollup;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;


public class DeleteRollupJobRequest implements Validatable, ToXContentObject {

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField DELETE_DATA_FIELD = new ParseField("delete_data");
    private final String id;
    private final boolean deleteData;

    public DeleteRollupJobRequest(String id) {
        this(id, false);
    }

    public DeleteRollupJobRequest(String id, boolean deleteData) {
        this.id = Objects.requireNonNull(id, "id parameter must not be null");
        this.deleteData = deleteData;
    }

    public String getId() {
        return id;
    }

    private static final ConstructingObjectParser<DeleteRollupJobRequest, Void> PARSER =
        new ConstructingObjectParser<>("request",  a -> new DeleteRollupJobRequest((String) a[0], (Boolean) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DELETE_DATA_FIELD);
    }

    public static DeleteRollupJobRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), this.id);
        builder.field(DELETE_DATA_FIELD.getPreferredName(), this.deleteData);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteRollupJobRequest that = (DeleteRollupJobRequest) o;
        return Objects.equals(id, that.id)
            && Objects.equals(deleteData, that.deleteData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, deleteData);
    }
}
