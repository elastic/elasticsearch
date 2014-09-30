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

package org.elasticsearch.action.mlt;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;

/**
 */
public class MoreLikeThisRequestBuilder extends ActionRequestBuilder<MoreLikeThisRequest, SearchResponse, MoreLikeThisRequestBuilder, Client> {

    public MoreLikeThisRequestBuilder(Client client) {
        super(client, new MoreLikeThisRequest());
    }

    public MoreLikeThisRequestBuilder(Client client, String index, String type, String id) {
        super(client, new MoreLikeThisRequest(index).type(type).id(id));
    }

    /**
     * The fields of the document to use in order to find documents "like" this one. Defaults to run
     * against all the document fields.
     */
    public MoreLikeThisRequestBuilder setField(String... fields) {
        request.fields(fields);
        return this;
    }

    /**
     * Sets the routing. Required if routing isn't id based.
     */
    public MoreLikeThisRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Number of terms that must match the generated query expressed in the
     * common syntax for minimum should match. Defaults to <tt>30%</tt>.
     *
     * @see    org.elasticsearch.common.lucene.search.Queries#calculateMinShouldMatch(int, String)
     */
    public MoreLikeThisRequestBuilder setMinimumShouldMatch(String minimumShouldMatch) {
        request.minimumShouldMatch(minimumShouldMatch);
        return this;
    }

    /**
     * The percent of the terms to match for each field. Defaults to <tt>0.3f</tt>.
     */
    public MoreLikeThisRequestBuilder setPercentTermsToMatch(float percentTermsToMatch) {
        return setMinimumShouldMatch(Math.round(percentTermsToMatch * 100) + "%");
    }

    /**
     * The frequency below which terms will be ignored in the source doc. Defaults to <tt>2</tt>.
     */
    public MoreLikeThisRequestBuilder setMinTermFreq(int minTermFreq) {
        request.minTermFreq(minTermFreq);
        return this;
    }

    /**
     * The maximum number of query terms that will be included in any generated query. Defaults to <tt>25</tt>.
     */
    public MoreLikeThisRequestBuilder maxQueryTerms(int maxQueryTerms) {
        request.maxQueryTerms(maxQueryTerms);
        return this;
    }

    /**
     * Any word in this set is considered "uninteresting" and ignored.
     * <p/>
     * <p>Even if your Analyzer allows stopwords, you might want to tell the MoreLikeThis code to ignore them, as
     * for the purposes of document similarity it seems reasonable to assume that "a stop word is never interesting".
     * <p/>
     * <p>Defaults to no stop words.
     */
    public MoreLikeThisRequestBuilder setStopWords(String... stopWords) {
        request.stopWords(stopWords);
        return this;
    }

    /**
     * The frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisRequestBuilder setMinDocFreq(int minDocFreq) {
        request.minDocFreq(minDocFreq);
        return this;
    }

    /**
     * The maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisRequestBuilder setMaxDocFreq(int maxDocFreq) {
        request.maxDocFreq(maxDocFreq);
        return this;
    }

    /**
     * The minimum word length below which words will be ignored. Defaults to <tt>0</tt>.
     */
    public MoreLikeThisRequestBuilder setMinWordLen(int minWordLen) {
        request.minWordLength(minWordLen);
        return this;
    }

    /**
     * The maximum word length above which words will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisRequestBuilder setMaxWordLen(int maxWordLen) {
        request().maxWordLength(maxWordLen);
        return this;
    }

    /**
     * The boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisRequestBuilder setBoostTerms(float boostTerms) {
        request.boostTerms(boostTerms);
        return this;
    }

    /**
     * Whether to include the queried document. Defaults to <tt>false</tt>.
     */
    public MoreLikeThisRequestBuilder setInclude(boolean include) {
        request.include(include);
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequestBuilder setSearchSource(SearchSourceBuilder sourceBuilder) {
        request.searchSource(sourceBuilder);
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequestBuilder setSearchSource(String searchSource) {
        request.searchSource(searchSource);
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequestBuilder setSearchSource(Map searchSource) {
        request.searchSource(searchSource);
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequestBuilder setSearchSource(XContentBuilder builder) {
        request.searchSource(builder);
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequestBuilder setSearchSource(byte[] searchSource) {
        request.searchSource(searchSource);
        return this;
    }

    /**
     * The search type of the mlt search query.
     */
    public MoreLikeThisRequestBuilder setSearchType(SearchType searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * The search type of the mlt search query.
     */
    public MoreLikeThisRequestBuilder setSearchType(String searchType) throws ElasticsearchIllegalArgumentException {
        request.searchType(searchType);
        return this;
    }

    /**
     * The indices the resulting mlt query will run against. If not set, will run
     * against the index the document was fetched from.
     */
    public MoreLikeThisRequestBuilder setSearchIndices(String... searchIndices) {
        request.searchIndices(searchIndices);
        return this;
    }

    /**
     * The types the resulting mlt query will run against. If not set, will run
     * against the type of the document fetched.
     */
    public MoreLikeThisRequestBuilder setSearchTypes(String... searchTypes) {
        request.searchTypes(searchTypes);
        return this;
    }

    /**
     * An optional search scroll request to be able to continue and scroll the search
     * operation.
     */
    public MoreLikeThisRequestBuilder setSearchScroll(Scroll searchScroll) {
        request.searchScroll(searchScroll);
        return this;
    }

    /**
     * The number of documents to return, defaults to 10.
     */
    public MoreLikeThisRequestBuilder setSearchSize(int size) {
        request.searchSize(size);
        return this;
    }

    /**
     * From which search result set to return.
     */
    public MoreLikeThisRequestBuilder setSearchFrom(int from) {
        request.searchFrom(from);
        return this;
    }


    @Override
    protected void doExecute(ActionListener<SearchResponse> listener) {
        client.moreLikeThis(request, listener);
    }
}
