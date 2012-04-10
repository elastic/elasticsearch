/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

    public String index() {
        return this.index;
    }

    public String getIndex() {
        return index();
    }

    public boolean valid() {
        return this.valid;
    }

    public boolean getValid() {
        return valid();
    }

    public String error() {
        return this.error;
    }

    public String getError() {
        return error();
    }

    public String explanation() {
        return this.explanation;
    }

    public String getExplanation() {
        return explanation();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readUTF();
        valid = in.readBoolean();
        explanation = in.readOptionalUTF();
        error = in.readOptionalUTF();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(index);
        out.writeBoolean(valid);
        out.writeOptionalUTF(explanation);
        out.writeOptionalUTF(error);
    }

    public static QueryExplanation readQueryExplanation(StreamInput in)  throws IOException {
        QueryExplanation exp = new QueryExplanation();
        exp.readFrom(in);
        return exp;
    }
}
