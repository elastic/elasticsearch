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

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines a QA specification: All end user supplied query intents will be mapped to the search request specified in this search request
 * template and executed against the targetIndex given. Any filters that should be applied in the target system can be specified as well.
 *
 * The resulting document lists can then be compared against what was specified in the set of rated documents as part of a QAQuery.  
 * */
public class Specification {

    private int specId = 0;
    private List<String> targetIndex = new ArrayList<String>();
    private String searchRequestTemplate = "";
    private BytesReference filter = new BytesArray("");
    
    /** Returns the index to send a query to. */
    public List<String> getTargetIndices() {
        return targetIndex;
    }


    /** Sets the index to send a query to. */
    public void setTargetIndices(List<String> targetIndex) {
        this.targetIndex = targetIndex;
    }


    /** Returns the search request including the search template to use for executing requests in this spec.*/
    public String getSearchRequestTemplate() {
        return searchRequestTemplate;
    }

    /** Sets the search request including the search template to use for executing requests in this spec.*/
    public void setSearchRequestTemplate(String searchRequestTemplate) {
        this.searchRequestTemplate = searchRequestTemplate;
    }


    /** Returns the filter to apply to requests sent under this spec. */
    public BytesReference getFilter() {
        return filter;
    }


    /** Sets the filter to apply to requests sent under this spec. */
    public void setFilter(BytesReference filter) {
        if (filter != null) {
            this.filter = filter;
        }
    }


    /** Returns a user supplied spec id for easier referencing. */
    public int getSpecId() {
        return specId;
    }

    /** Sets a user supplied spec id for easier referencing. */
    public void setSpecId(int specId) {
        this.specId = specId;
    }


    @Override
    public String toString() {
        ToStringHelper help = MoreObjects.toStringHelper(this).add("Target index", targetIndex);
        help.add("Template SearchRequest", searchRequestTemplate);
        help.add("Filter", filter.toUtf8());
        help.add("Spec ID", specId);
        return help.toString();
    }
}
