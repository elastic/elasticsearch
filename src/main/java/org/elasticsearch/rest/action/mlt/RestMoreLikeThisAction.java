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

package org.elasticsearch.rest.action.mlt;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.Scroll;

import java.io.IOException;

import static org.elasticsearch.client.Requests.moreLikeThisRequest;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 *
 */
public class RestMoreLikeThisAction extends BaseRestHandler {

    @Inject
    public RestMoreLikeThisAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_mlt", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_mlt", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        MoreLikeThisRequest mltRequest = moreLikeThisRequest(request.param("index")).type(request.param("type")).id(request.param("id"));
        mltRequest.listenerThreaded(false);
        try {
            mltRequest.fields(request.paramAsStringArray("mlt_fields", null));
            mltRequest.percentTermsToMatch(request.paramAsFloat("percent_terms_to_match", -1));
            mltRequest.minTermFreq(request.paramAsInt("min_term_freq", -1));
            mltRequest.maxQueryTerms(request.paramAsInt("max_query_terms", -1));
            mltRequest.stopWords(request.paramAsStringArray("stop_words", null));
            mltRequest.minDocFreq(request.paramAsInt("min_doc_freq", -1));
            mltRequest.maxDocFreq(request.paramAsInt("max_doc_freq", -1));
            mltRequest.minWordLen(request.paramAsInt("min_word_len", -1));
            mltRequest.maxWordLen(request.paramAsInt("max_word_len", -1));
            mltRequest.boostTerms(request.paramAsFloat("boost_terms", -1));

            mltRequest.searchType(SearchType.fromString(request.param("search_type")));
            mltRequest.searchIndices(request.paramAsStringArray("search_indices", null));
            mltRequest.searchTypes(request.paramAsStringArray("search_types", null));
            mltRequest.searchQueryHint(request.param("search_query_hint"));
            mltRequest.searchSize(request.paramAsInt("search_size", mltRequest.searchSize()));
            mltRequest.searchFrom(request.paramAsInt("search_from", mltRequest.searchFrom()));
            String searchScroll = request.param("search_scroll");
            if (searchScroll != null) {
                mltRequest.searchScroll(new Scroll(parseTimeValue(searchScroll, null)));
            }
            if (request.hasContent()) {
                mltRequest.searchSource(request.content(), request.contentUnsafe());
            } else {
                String searchSource = request.param("search_source");
                if (searchSource != null) {
                    mltRequest.searchSource(searchSource);
                }
            }
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.moreLikeThis(mltRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
