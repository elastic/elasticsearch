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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class QueryExplanation  implements Streamable {

    public static final int RANDOM_SHARD = -1;

    private String index;

    private int shard = RANDOM_SHARD;

    private boolean valid;

    private String explanation;

    private String error;

    QueryExplanation() {

    }

    public QueryExplanation(String index, int shard, boolean valid, String explanation,
                            String error) {
        this.index = index;
        this.shard = shard;
        this.valid = valid;
        this.explanation = explanation;
        this.error = error;
    }

    public String getIndex() {
        return this.index;
    }

    public int getShard() {
        return this.shard;
    }

    public boolean isValid() {
        return this.valid;
    }

    public String getError() {
        return this.error;
    }

    public String getExplanation() {
        return this.explanation;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        if (in.getVersion().onOrAfter(Version.V_5_4_0)) {
            shard = in.readInt();
        } else {
            shard = RANDOM_SHARD;
        }
        valid = in.readBoolean();
        explanation = in.readOptionalString();
        error = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        if (out.getVersion().onOrAfter(Version.V_5_4_0)) {
            out.writeInt(shard);
        }
        out.writeBoolean(valid);
        out.writeOptionalString(explanation);
        out.writeOptionalString(error);
    }

    public static QueryExplanation readQueryExplanation(StreamInput in)  throws IOException {
        QueryExplanation exp = new QueryExplanation();
        exp.readFrom(in);
        return exp;
    }
}
