/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.IndexReplicationOperationRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.util.Required;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexDeleteByQueryRequest extends IndexReplicationOperationRequest {

    private String querySource;
    private String queryParserName;
    private String[] types = Strings.EMPTY_ARRAY;

    public IndexDeleteByQueryRequest(String index, String... types) {
        this.index = index;
        this.types = types;
    }

    IndexDeleteByQueryRequest(DeleteByQueryRequest request, String index) {
        this.index = index;
        this.timeout = request.timeout();
        this.querySource = request.querySource();
        this.queryParserName = request.queryParserName();
        this.types = request.types();
    }


    IndexDeleteByQueryRequest() {
    }

    String querySource() {
        return querySource;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (querySource == null) {
            validationException = addValidationError("querySource is missing", validationException);
        }
        return validationException;
    }

    @Required public IndexDeleteByQueryRequest querySource(QueryBuilder queryBuilder) {
        return querySource(queryBuilder.build());
    }

    @Required public IndexDeleteByQueryRequest querySource(String querySource) {
        this.querySource = querySource;
        return this;
    }

    String queryParserName() {
        return queryParserName;
    }

    String[] types() {
        return this.types;
    }

    public IndexDeleteByQueryRequest queryParserName(String queryParserName) {
        this.queryParserName = queryParserName;
        return this;
    }

    public IndexDeleteByQueryRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        querySource = in.readUTF();
        if (in.readBoolean()) {
            queryParserName = in.readUTF();
        }
        int typesSize = in.readInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(querySource);
        if (queryParserName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryParserName);
        }
        out.writeInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}