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

package org.elasticsearch.action.mlt;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.query.json.BoolJsonQueryBuilder;
import org.elasticsearch.index.query.json.MoreLikeThisFieldJsonQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.Set;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportMoreLikeThisAction extends BaseAction<MoreLikeThisRequest, SearchResponse> {

    private final TransportSearchAction searchAction;

    private final TransportGetAction getAction;

    private final IndicesService indicesService;

    @Inject public TransportMoreLikeThisAction(Settings settings, TransportSearchAction searchAction, TransportGetAction getAction,
                                               IndicesService indicesService, TransportService transportService) {
        super(settings);
        this.searchAction = searchAction;
        this.getAction = getAction;
        this.indicesService = indicesService;

        transportService.registerHandler(TransportActions.MORE_LIKE_THIS, new TransportHandler());
    }

    @Override protected void doExecute(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        GetRequest getRequest = getRequest(request.index())
                .type(request.type())
                .id(request.id())
                .listenerThreaded(false);
        getAction.execute(getRequest, new ActionListener<GetResponse>() {
            @Override public void onResponse(GetResponse getResponse) {
                if (getResponse.empty()) {
                    listener.onFailure(new ElasticSearchException("document missing"));
                    return;
                }
                final BoolJsonQueryBuilder boolBuilder = boolQuery();
                try {
                    DocumentMapper docMapper = indicesService.indexServiceSafe(request.index()).mapperService().documentMapper(request.type());
                    final Set<String> fields = Sets.newHashSet();
                    if (request.fields() != null) {
                        for (String field : request.fields()) {
                            FieldMappers fieldMappers = docMapper.mappers().smartName(field);
                            if (fieldMappers != null) {
                                fields.add(fieldMappers.mapper().names().indexName());
                            } else {
                                fields.add(field);
                            }
                        }
                    }
                    docMapper.parse(request.type(), request.id(), getResponse.source(), new DocumentMapper.ParseListenerAdapter() {
                        @Override public boolean beforeFieldAdded(FieldMapper fieldMapper, Fieldable field, Object parseContext) {
                            if (fieldMapper instanceof InternalMapper) {
                                return true;
                            }
                            String value = fieldMapper.valueAsString(field);
                            if (value == null) {
                                return false;
                            }

                            if (fields.isEmpty() || fields.contains(field.name())) {
                                addMoreLikeThis(request, boolBuilder, fieldMapper, field);
                            }

                            return false;
                        }
                    });
                    // exclude myself
                    Term uidTerm = docMapper.uidMapper().term(request.type(), request.id());
                    boolBuilder.mustNot(termQuery(uidTerm.field(), uidTerm.text()));
                } catch (Exception e) {
                    listener.onFailure(e);
                }

                String[] searchIndices = request.searchIndices();
                if (searchIndices == null) {
                    searchIndices = new String[]{request.index()};
                }
                String[] searchTypes = request.searchTypes();
                if (searchTypes == null) {
                    searchTypes = new String[]{request.type()};
                }

                SearchRequest searchRequest = searchRequest(searchIndices)
                        .types(searchTypes)
                        .searchType(request.searchType())
                        .source(request.searchSource())
                        .scroll(request.searchScroll())
                        .extraSource(searchSource()
                                .query(boolBuilder)
                        )
                        .listenerThreaded(request.listenerThreaded());
                searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
                    @Override public void onResponse(SearchResponse response) {
                        listener.onResponse(response);
                    }

                    @Override public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });

            }

            @Override public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    private void addMoreLikeThis(MoreLikeThisRequest request, BoolJsonQueryBuilder boolBuilder, FieldMapper fieldMapper, Fieldable field) {
        MoreLikeThisFieldJsonQueryBuilder mlt = moreLikeThisFieldQuery(field.name())
                .likeText(fieldMapper.valueAsString(field))
                .percentTermsToMatch(request.percentTermsToMatch())
                .boostTerms(request.boostTerms())
                .boostTermsFactor(request.boostTermsFactor())
                .minDocFreq(request.minDocFreq())
                .maxDocFreq(request.maxDocFreq())
                .minWordLen(request.minWordLen())
                .maxWordLen(request.maxWordLen())
                .minTermFrequency(request.minTermFrequency())
                .maxQueryTerms(request.maxQueryTerms())
                .stopWords(request.stopWords());
        boolBuilder.should(mlt);
    }

    private class TransportHandler extends BaseTransportRequestHandler<MoreLikeThisRequest> {

        @Override public MoreLikeThisRequest newInstance() {
            return new MoreLikeThisRequest();
        }

        @Override public void messageReceived(MoreLikeThisRequest request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<SearchResponse>() {
                @Override public void onResponse(SearchResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }
}
