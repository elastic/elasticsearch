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

package org.elasticsearch.rest.action.mlt;

import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.search.Scroll;

import static org.elasticsearch.client.Requests.moreLikeThisRequest;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestMoreLikeThisAction extends BaseRestHandler {

    @Inject
    public RestMoreLikeThisAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_mlt", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_mlt", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        MoreLikeThisRequest mltRequest = moreLikeThisRequest(request.param("index")).type(request.param("type")).id(request.param("id"));
        mltRequest.routing(request.param("routing"));

        mltRequest.listenerThreaded(false);
        //TODO the ParseField class that encapsulates the supported names used for an attribute
        //needs some work if it is to be used in a REST context like this too
        // See the MoreLikeThisQueryParser constants that hold the valid syntax
        mltRequest.fields(request.paramAsStringArray("mlt_fields", null));
        mltRequest.minimumShouldMatch(request.param("minimum_should_match", "0"));
        mltRequest.minTermFreq(request.paramAsInt("min_term_freq", -1));
        mltRequest.maxQueryTerms(request.paramAsInt("max_query_terms", -1));
        mltRequest.stopWords(request.paramAsStringArray("stop_words", null));
        mltRequest.minDocFreq(request.paramAsInt("min_doc_freq", -1));
        mltRequest.maxDocFreq(request.paramAsInt("max_doc_freq", -1));
        mltRequest.minWordLength(request.paramAsInt("min_word_len", request.paramAsInt("min_word_length", -1)));
        mltRequest.maxWordLength(request.paramAsInt("max_word_len", request.paramAsInt("max_word_length", -1)));
        mltRequest.boostTerms(request.paramAsFloat("boost_terms", -1));
        mltRequest.include(request.paramAsBoolean("include", false));

        mltRequest.searchType(SearchType.fromString(request.param("search_type")));
        mltRequest.searchIndices(request.paramAsStringArray("search_indices", null));
        mltRequest.searchTypes(request.paramAsStringArray("search_types", null));
        mltRequest.searchSize(request.paramAsInt("search_size", mltRequest.searchSize()));
        mltRequest.searchFrom(request.paramAsInt("search_from", mltRequest.searchFrom()));
        String searchScroll = request.param("search_scroll");
        if (searchScroll != null) {
            mltRequest.searchScroll(new Scroll(parseTimeValue(searchScroll, null)));
        }
        if (request.hasContent()) {
            mltRequest.searchSource(request.content());
        } else {
            String searchSource = request.param("search_source");
            if (searchSource != null) {
                mltRequest.searchSource(searchSource);
            }
        }

        client.moreLikeThis(mltRequest, new RestToXContentListener<SearchResponse>(channel));
    }
}
