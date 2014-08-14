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

package org.elasticsearch.action.quality;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class PrecisionAtRequest extends ActionRequest<PrecisionAtRequest> {

    /** TODO move the following to a metric specific context - need move writing and reading too then. */

    /** IDs of documents considered relevant for this query. */
    private Collection<String> relevantDocs = new LinkedList<String>();
    /** QueryBuilder to run against search index and generate hits to compare against relevantDocs. */
    private SearchRequest searchRequest;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public void searchRequestBuilder(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    public void relevantDocs(Collection<String> relevant) {
        this.relevantDocs.addAll(relevant);
    }
    
    public SearchRequest getSearchRequest() {
        return searchRequest;
    }
    
    public Collection<String> getRelevantDocs() {
        return relevantDocs;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        searchRequest = new SearchRequest();
        searchRequest.readFrom(in);
        relevantDocs = (List<String>) in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        searchRequest.writeTo(out);
        out.writeGenericValue(relevantDocs);
    }
    
    @Override
    public String toString() {
        ToStringHelper help = Objects.toStringHelper(this).add("Relevant docs", relevantDocs.toString());
        help.add("SearchRequest", searchRequest);
        return help.toString();
    }
}
