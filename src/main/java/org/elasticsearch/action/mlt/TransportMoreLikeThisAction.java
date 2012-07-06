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

package org.elasticsearch.action.mlt;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisFieldQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 * The more like this action.
 */
public class TransportMoreLikeThisAction extends TransportAction<MoreLikeThisRequest, SearchResponse> {

    private final TransportSearchAction searchAction;

    private final TransportGetAction getAction;

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    @Inject
    public TransportMoreLikeThisAction(Settings settings, ThreadPool threadPool, TransportSearchAction searchAction, TransportGetAction getAction,
                                       ClusterService clusterService, IndicesService indicesService, TransportService transportService) {
        super(settings, threadPool);
        this.searchAction = searchAction;
        this.getAction = getAction;
        this.indicesService = indicesService;
        this.clusterService = clusterService;

        transportService.registerHandler(MoreLikeThisAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        // update to actual index name
        ClusterState clusterState = clusterService.state();
        // update to the concrete index
        final String concreteIndex = clusterState.metaData().concreteIndex(request.index());

        Set<String> getFields = newHashSet();
        if (request.fields() != null) {
            Collections.addAll(getFields, request.fields());
        }
        // add the source, in case we need to parse it to get fields
        getFields.add(SourceFieldMapper.NAME);

        GetRequest getRequest = getRequest(concreteIndex)
                .fields(getFields.toArray(new String[getFields.size()]))
                .type(request.type())
                .id(request.id())
                .listenerThreaded(true)
                .operationThreaded(true);

        request.beforeLocalFork();
        getAction.execute(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (!getResponse.exists()) {
                    listener.onFailure(new ElasticSearchException("document missing"));
                    return;
                }
                final BoolQueryBuilder boolBuilder = boolQuery();
                try {
                    DocumentMapper docMapper = indicesService.indexServiceSafe(concreteIndex).mapperService().documentMapper(request.type());
                    final Set<String> fields = newHashSet();
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

                    if (!fields.isEmpty()) {
                        // if fields are not empty, see if we got them in the response
                        for (Iterator<String> it = fields.iterator(); it.hasNext(); ) {
                            String field = it.next();
                            GetField getField = getResponse.field(field);
                            if (getField != null) {
                                for (Object value : getField.values()) {
                                    addMoreLikeThis(request, boolBuilder, getField.name(), value.toString());
                                }
                                it.remove();
                            }
                        }
                        if (!fields.isEmpty()) {
                            // if we don't get all the fields in the get response, see if we can parse the source
                            parseSource(getResponse, boolBuilder, docMapper, fields, request);
                        }
                    } else {
                        // we did not ask for any fields, try and get it from the source
                        parseSource(getResponse, boolBuilder, docMapper, fields, request);
                    }

                    if (!boolBuilder.hasClauses()) {
                        // no field added, fail
                        listener.onFailure(new ElasticSearchException("No fields found to fetch the 'likeText' from"));
                        return;
                    }

                    // exclude myself
                    Term uidTerm = docMapper.uidMapper().term(request.type(), request.id());
                    boolBuilder.mustNot(termQuery(uidTerm.field(), uidTerm.text()));
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }

                String[] searchIndices = request.searchIndices();
                if (searchIndices == null) {
                    searchIndices = new String[]{request.index()};
                }
                String[] searchTypes = request.searchTypes();
                if (searchTypes == null) {
                    searchTypes = new String[]{request.type()};
                }
                int size = request.searchSize() != 0 ? request.searchSize() : 10;
                int from = request.searchFrom() != 0 ? request.searchFrom() : 0;
                SearchRequest searchRequest = searchRequest(searchIndices)
                        .types(searchTypes)
                        .searchType(request.searchType())
                        .scroll(request.searchScroll())
                        .extraSource(searchSource()
                                .query(boolBuilder)
                                .from(from)
                                .size(size)
                        )
                        .listenerThreaded(request.listenerThreaded());

                if (request.searchSource() != null) {
                    searchRequest.source(request.searchSource(), request.searchSourceUnsafe());
                }
                searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });

            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    private void parseSource(GetResponse getResponse, final BoolQueryBuilder boolBuilder, DocumentMapper docMapper, final Set<String> fields, final MoreLikeThisRequest request) {
        if (getResponse.source() == null) {
            return;
        }
        docMapper.parse(SourceToParse.source(getResponse.sourceRef()).type(request.type()).id(request.id()), new DocumentMapper.ParseListenerAdapter() {
            @Override
            public boolean beforeFieldAdded(FieldMapper fieldMapper, Fieldable field, Object parseContext) {
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
    }

    private void addMoreLikeThis(MoreLikeThisRequest request, BoolQueryBuilder boolBuilder, FieldMapper fieldMapper, Fieldable field) {
        addMoreLikeThis(request, boolBuilder, field.name(), fieldMapper.valueAsString(field));
    }

    private void addMoreLikeThis(MoreLikeThisRequest request, BoolQueryBuilder boolBuilder, String fieldName, String likeText) {
        MoreLikeThisFieldQueryBuilder mlt = moreLikeThisFieldQuery(fieldName)
                .likeText(likeText)
                .percentTermsToMatch(request.percentTermsToMatch())
                .boostTerms(request.boostTerms())
                .minDocFreq(request.minDocFreq())
                .maxDocFreq(request.maxDocFreq())
                .minWordLen(request.minWordLen())
                .maxWordLen(request.maxWordLen())
                .minTermFreq(request.minTermFreq())
                .maxQueryTerms(request.maxQueryTerms())
                .stopWords(request.stopWords());
        boolBuilder.should(mlt);
    }

    private class TransportHandler extends BaseTransportRequestHandler<MoreLikeThisRequest> {

        @Override
        public MoreLikeThisRequest newInstance() {
            return new MoreLikeThisRequest();
        }

        @Override
        public void messageReceived(MoreLikeThisRequest request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
