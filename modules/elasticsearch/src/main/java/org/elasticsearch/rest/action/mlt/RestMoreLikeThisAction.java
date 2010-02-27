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

package org.elasticsearch.rest.action.mlt;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestResponse.Status.*;
import static org.elasticsearch.rest.action.support.RestActions.*;
import static org.elasticsearch.rest.action.support.RestJsonBuilder.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class RestMoreLikeThisAction extends BaseRestHandler {

    @Inject public RestMoreLikeThisAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_moreLikeThis", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_moreLikeThis", this);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_mlt", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_mlt", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        MoreLikeThisRequest mltRequest = moreLikeThisRequest(request.param("index")).type(request.param("type")).id(request.param("id"));
        try {
            mltRequest.fields(request.paramAsStringArray("fields", null));
            mltRequest.percentTermsToMatch(request.paramAsFloat("percentTermsToMatch", -1));
            mltRequest.minTermFrequency(request.paramAsInt("minTermFrequency", -1));
            mltRequest.maxQueryTerms(request.paramAsInt("maxQueryTerms", -1));
            mltRequest.stopWords(request.paramAsStringArray("stopWords", null));
            mltRequest.minDocFreq(request.paramAsInt("minDocFreq", -1));
            mltRequest.maxDocFreq(request.paramAsInt("maxDocFreq", -1));
            mltRequest.minWordLen(request.paramAsInt("minWordLen", -1));
            mltRequest.maxWordLen(request.paramAsInt("maxWordLen", -1));
            mltRequest.boostTerms(request.paramAsBoolean("boostTerms", null));
            mltRequest.boostTermsFactor(request.paramAsFloat("boostTermsFactor", -1));

            mltRequest.searchType(parseSearchType(request.param("searchType")));
            mltRequest.searchIndices(request.paramAsStringArray("searchIndices", null));
            mltRequest.searchTypes(request.paramAsStringArray("searchTypes", null));
            mltRequest.searchQueryHint(request.param("searchQueryHint"));
            String searchScroll = request.param("searchScroll");
            if (searchScroll != null) {
                mltRequest.searchScroll(new Scroll(parseTimeValue(searchScroll, null)));
            }
            if (request.hasContent()) {
                mltRequest.searchSource(request.contentAsBytes());
            }
        } catch (Exception e) {
            try {
                JsonBuilder builder = restJsonBuilder(request);
                channel.sendResponse(new JsonRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.moreLikeThis(mltRequest, new ActionListener<SearchResponse>() {
            @Override public void onResponse(SearchResponse response) {
                try {
                    JsonBuilder builder = restJsonBuilder(request);
                    builder.startObject();
                    response.toJson(builder, request);
                    builder.endObject();
                    channel.sendResponse(new JsonRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
