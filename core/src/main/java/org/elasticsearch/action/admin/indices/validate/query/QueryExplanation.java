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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 *
 */
public class QueryExplanation  implements Streamable {

    private String index;
    
    private boolean valid;
    
    private String explanation;
    
    private String error;

    QueryExplanation() {
        
    }
    
    public QueryExplanation(String index, boolean valid, String explanation, String error) {
        this.index = index;
        this.valid = valid;
        this.explanation = explanation;
        this.error = error;
    }

    public String getIndex() {
        return this.index;
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
        valid = in.readBoolean();
        explanation = in.readOptionalString();
        error = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
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
