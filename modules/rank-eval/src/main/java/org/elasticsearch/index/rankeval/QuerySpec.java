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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines a QA specification: All end user supplied query intents will be mapped to the search request specified in this search request
 * template and executed against the targetIndex given. Any filters that should be applied in the target system can be specified as well.
 *
 * The resulting document lists can then be compared against what was specified in the set of rated documents as part of a QAQuery.  
 * */
public class QuerySpec implements Writeable {

    private int specId = 0;
    private SearchSourceBuilder testRequest;
    private Template template;
    private List<String> indices = new ArrayList<>();
    private List<String> types = new ArrayList<>();
    
    public QuerySpec(
            int specId, SearchSourceBuilder testRequest, List<String> indices, List<String> types, Template template) {
        this.specId = specId;
        this.testRequest = testRequest;
        this.indices = indices;
        this.types = types;
        this.template = template;
    }

    public QuerySpec(StreamInput in) throws IOException {
        this.specId = in.readInt();
        testRequest = new SearchSourceBuilder(in);
        int indicesSize = in.readInt();
        indices = new ArrayList<String>(indicesSize);
        for (int i = 0; i < indicesSize; i++) {
            this.indices.add(in.readString());
        }
        int typesSize = in.readInt();
        types = new ArrayList<String>(typesSize);
        for (int i = 0; i < typesSize; i++) {
            this.types.add(in.readString());
        }
        this.template = new Template(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(specId);
        testRequest.writeTo(out);
        out.writeInt(indices.size());
        for (String index : indices) {
            out.writeString(index);
        }
        out.writeInt(types.size());
        for (String type : types) {
            out.writeString(type);
        }
        this.template.writeTo(out);
    }

    public SearchSourceBuilder getTestRequest() {
        return testRequest;
    }
    
    public Template getTemplate() {
        return template;
    }

    public void setTestRequest(SearchSourceBuilder testRequest) {
        this.testRequest = testRequest;
    }

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
    }

    /** Returns a user supplied spec id for easier referencing. */
    public int getSpecId() {
        return specId;
    }

    /** Sets a user supplied spec id for easier referencing. */
    public void setSpecId(int specId) {
        this.specId = specId;
    }
}
