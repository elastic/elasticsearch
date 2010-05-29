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

package org.elasticsearch.client.action.terms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.action.terms.TermsRequest;
import org.elasticsearch.action.terms.TermsResponse;
import org.elasticsearch.client.internal.InternalClient;

/**
 * @author kimchy (shay.banon)
 */
public class TermsRequestBuilder {

    private final InternalClient client;

    private final TermsRequest request;

    public TermsRequestBuilder(InternalClient client) {
        this.client = client;
        this.request = new TermsRequest();
    }

    /**
     * Sets the indices the terms will run against.
     */
    public TermsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The fields within each document which terms will be iterated over and returned with the
     * document frequencies. By default will use the "_all" field.
     */
    public TermsRequestBuilder setFields(String... fields) {
        request.fields(fields);
        return this;
    }

    /**
     * The lower bound term from which the iteration will start.  Defaults to start from the
     * first.
     */
    public TermsRequestBuilder setFrom(Object from) {
        request.from(from);
        return this;
    }

    /**
     * Greater than (like setting from with fromIInclusive set to <tt>false</tt>).
     */
    public TermsRequestBuilder setGreaterThan(Object from) {
        request.gt(from);
        return this;
    }

    /**
     * Greater/equal than  (like setting from with fromInclusive set to <tt>true</tt>).
     */
    public TermsRequestBuilder setGreaterEqualsThan(Object from) {
        request.gt(from);
        return this;
    }

    /**
     * Lower then (like setting to with toInclusive set to <tt>false</tt>)
     */
    public TermsRequestBuilder setLowerThan(Object to) {
        request.lt(to);
        return this;
    }

    /**
     * Lower/equal then (like setting to with toInclusive set to <tt>false</tt>)
     */
    public TermsRequestBuilder setLowerEqualThan(Object to) {
        request.lte(to);
        return this;
    }

    /**
     * Should the first from (if set using {@link #setFrom(Object)} be inclusive or not. Defaults
     * to <tt>false</tt> (not inclusive / exclusive).
     */
    public TermsRequestBuilder setFromInclusive(boolean fromInclusive) {
        request.fromInclusive(fromInclusive);
        return this;
    }

    /**
     * The upper bound term to which the iteration will end. Defaults to unbound (<tt>null</tt>).
     */
    public TermsRequestBuilder setTo(Object to) {
        request.to(to);
        return this;
    }

    /**
     * Should the last to (if set using {@link #setTo(Object)} be inclusive or not. Defaults to
     * <tt>true</tt>.
     */
    public TermsRequestBuilder setToInclusive(boolean toInclusive) {
        request.toInclusive(toInclusive);
        return this;
    }

    /**
     * An optional prefix from which the terms iteration will start (in lex order).
     */
    public TermsRequestBuilder setPrefix(String prefix) {
        request.prefix(prefix);
        return this;
    }

    /**
     * An optional regular expression to filter out terms (only the ones that match the regexp
     * will return).
     */
    public TermsRequestBuilder setRegexp(String regexp) {
        request.regexp(regexp);
        return this;
    }

    /**
     * An optional minimum document frequency to filter out terms.
     */
    public TermsRequestBuilder setMinFreq(int minFreq) {
        request.minFreq(minFreq);
        return this;
    }

    /**
     * An optional maximum document frequency to filter out terms.
     */
    public TermsRequestBuilder setMaxFreq(int maxFreq) {
        request.maxFreq(maxFreq);
        return this;
    }

    /**
     * The number of term / doc freq pairs to return per field. Defaults to <tt>10</tt>.
     */
    public TermsRequestBuilder setSize(int size) {
        request.size(size);
        return this;
    }

    /**
     * The type of sorting for term / doc freq. Can either sort on term (lex) or doc frequency. Defaults to
     * {@link TermsRequest.SortType#TERM}.
     */
    public TermsRequestBuilder setSortType(TermsRequest.SortType sortType) {
        request.sortType(sortType);
        return this;
    }

    /**
     * Sets the string representation of the sort type.
     */
    public TermsRequestBuilder setSortType(String sortType) {
        request.sortType(sortType);
        return this;
    }

    /**
     * Should the doc frequencies be exact frequencies. Exact frequencies takes into account deletes that
     * have not been merged and cleaned (optimized). Note, when this is set to <tt>true</tt> this operation
     * might be an expensive operation. Defaults to <tt>false</tt>.
     */
    public TermsRequestBuilder setExact(boolean exact) {
        request.exact(exact);
        return this;
    }

    /**
     * Executes the operation asynchronously and returns a future.
     */
    public ListenableActionFuture<TermsResponse> execute() {
        PlainListenableActionFuture<TermsResponse> future = new PlainListenableActionFuture<TermsResponse>(request.listenerThreaded(), client.threadPool());
        client.terms(request, future);
        return future;
    }

    /**
     * Executes the operation asynchronously with the provided listener.
     */
    public void execute(ActionListener<TermsResponse> listener) {
        client.terms(request, listener);
    }
}
