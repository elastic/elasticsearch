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

package org.elasticsearch.action.termvectors.dfs;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class DfsOnlyRequest extends BroadcastRequest<DfsOnlyRequest> {

    private SearchRequest searchRequest = new SearchRequest();

    long nowInMillis;

    public DfsOnlyRequest() {

    }

    public DfsOnlyRequest(Fields termVectorsFields, String[] indices, String[] types, Set<String> selectedFields) throws IOException {
        super(indices);

        // build a search request with a query of all the terms
        final BoolQueryBuilder boolBuilder = boolQuery();
        for (String fieldName : termVectorsFields) {
            if ((selectedFields != null) && (!selectedFields.contains(fieldName))) {
                continue;
            }
            Terms terms = termVectorsFields.terms(fieldName);
            TermsEnum iterator = terms.iterator();
            while (iterator.next() != null) {
                String text = iterator.term().utf8ToString();
                boolBuilder.should(QueryBuilders.termQuery(fieldName, text));
            }
        }
        // wrap a search request object
        this.searchRequest = new SearchRequest(indices).types(types).source(new SearchSourceBuilder().query(boolBuilder));
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        return searchRequest.validate();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.searchRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.searchRequest.writeTo(out);
    }

    public String[] types() {
        return this.searchRequest.types();
    }

    public String routing() {
        return this.searchRequest.routing();
    }

    public String preference() {
        return this.searchRequest.preference();
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(searchRequest.source(), false);
        } catch (IOException e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types()) + ", source[" + sSource + "]";
    }

}
