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


package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;

public class SubmitAsyncSearchRequest extends TimedRequest {

    private TimeValue waitForCompletion;
    private Boolean cleanOnCompletion;
    private TimeValue keepAlive;
    private final SearchRequest searchRequest;

    public SubmitAsyncSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
}

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(TimeValue waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public Boolean isCleanOnCompletion() {
        return cleanOnCompletion;
    }

    public void setCleanOnCompletion(boolean cleanOnCompletion) {
        this.cleanOnCompletion = cleanOnCompletion;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubmitAsyncSearchRequest request = (SubmitAsyncSearchRequest) o;
        return Objects.equals(getSearchRequest(), request.getSearchRequest())
                && Objects.equals(getKeepAlive(), request.getKeepAlive())
                && Objects.equals(getWaitForCompletion(), request.getWaitForCompletion())
                && Objects.equals(isCleanOnCompletion(), request.isCleanOnCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSearchRequest(), getKeepAlive(), getWaitForCompletion(), isCleanOnCompletion());
    }
}
