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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 * The more like this action.
 */
public class TransportMoreLikeThisAction extends HandledTransportAction<MoreLikeThisRequest, SearchResponse> {

    private final TransportSearchAction searchAction;
    private final TransportGetAction getAction;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportMoreLikeThisAction(Settings settings, ThreadPool threadPool, TransportSearchAction searchAction, TransportGetAction getAction,
                                       ClusterService clusterService, IndicesService indicesService, TransportService transportService, ActionFilters actionFilters) {
        super(settings, MoreLikeThisAction.NAME, threadPool, transportService, actionFilters, MoreLikeThisRequest.class);
        this.searchAction = searchAction;
        this.getAction = getAction;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        // update to actual index name
        ClusterState clusterState = clusterService.state();
        // update to the concrete index
        final String concreteIndex = clusterState.metaData().concreteSingleIndex(request.index(), request.indicesOptions());

        Iterable<MutableShardRouting> routingNode = clusterState.getRoutingNodes().routingNodeIter(clusterService.localNode().getId());
        if (routingNode == null) {
            redirect(request, concreteIndex, listener, clusterState);
            return;
        }
        boolean hasIndexLocally = false;
        for (MutableShardRouting shardRouting : routingNode) {
            if (concreteIndex.equals(shardRouting.index())) {
                hasIndexLocally = true;
                break;
            }
        }
        if (!hasIndexLocally) {
            redirect(request, concreteIndex, listener, clusterState);
            return;
        }
        Set<String> getFields = newHashSet();
        if (request.fields() != null) {
            Collections.addAll(getFields, request.fields());
        }
        // add the source, in case we need to parse it to get fields
        getFields.add(SourceFieldMapper.NAME);

        GetRequest getRequest = new GetRequest(request, request.index())
                .fields(getFields.toArray(new String[getFields.size()]))
                .type(request.type())
                .id(request.id())
                .routing(request.routing())
                .listenerThreaded(true)
                .operationThreaded(true);

        getAction.execute(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (!getResponse.isExists()) {
                    listener.onFailure(new DocumentMissingException(null, request.type(), request.id()));
                    return;
                }
                final BoolQueryBuilder boolBuilder = boolQuery();
                try {
                    final DocumentMapper docMapper = indicesService.indexServiceSafe(concreteIndex).mapperService().documentMapper(request.type());
                    if (docMapper == null) {
                        throw new ElasticsearchException("No DocumentMapper found for type [" + request.type() + "]");
                    }
                    final Set<String> fields = newHashSet();
                    if (request.fields() != null) {
                        for (String field : request.fields()) {
                            FieldMapper fieldMapper = docMapper.mappers().smartNameFieldMapper(field);
                            if (fieldMapper != null) {
                                fields.add(fieldMapper.names().indexName());
                            } else {
                                fields.add(field);
                            }
                        }
                    }

                    if (!fields.isEmpty()) {
                        // if fields are not empty, see if we got them in the response
                        for (Iterator<String> it = fields.iterator(); it.hasNext(); ) {
                            String field = it.next();
                            GetField getField = getResponse.getField(field);
                            if (getField != null) {
                                for (Object value : getField.getValues()) {
                                    addMoreLikeThis(request, boolBuilder, getField.getName(), value.toString(), true);
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
                        listener.onFailure(new ElasticsearchException("No fields found to fetch the 'likeText' from"));
                        return;
                    }

                    // exclude myself
                    if (!request.include()) {
                        Term uidTerm = docMapper.uidMapper().term(request.type(), request.id());
                        boolBuilder.mustNot(termQuery(uidTerm.field(), uidTerm.text()));
                        boolBuilder.adjustPureNegative(false);
                    }
                } catch (Throwable e) {
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

                SearchRequest searchRequest = new SearchRequest(request).indices(searchIndices)
                        .types(searchTypes)
                        .searchType(request.searchType())
                        .scroll(request.searchScroll())
                        .listenerThreaded(request.listenerThreaded());

                SearchSourceBuilder extraSource = searchSource().query(boolBuilder);
                if (request.searchFrom() != 0) {
                    extraSource.from(request.searchFrom());
                }
                if (request.searchSize() != 0) {
                    extraSource.size(request.searchSize());
                }
                searchRequest.extraSource(extraSource);

                if (request.searchSource() != null) {
                    searchRequest.source(request.searchSource());
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

    // Redirects the request to a data node, that has the index meta data locally available.
    private void redirect(MoreLikeThisRequest request, String concreteIndex, final ActionListener<SearchResponse> listener, ClusterState clusterState) {
        ShardIterator shardIterator = clusterService.operationRouting().getShards(clusterState, concreteIndex, request.type(), request.id(), request.routing(), null);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        if (shardRouting == null) {
            throw new ElasticsearchException("No shards for index " + request.index());
        }
        String nodeId = shardRouting.currentNodeId();
        DiscoveryNode discoveryNode = clusterState.nodes().get(nodeId);
        transportService.sendRequest(discoveryNode, MoreLikeThisAction.NAME, request, new TransportResponseHandler<SearchResponse>() {

            @Override
            public SearchResponse newInstance() {
                return new SearchResponse();
            }

            @Override
            public void handleResponse(SearchResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });
    }

    private void parseSource(GetResponse getResponse, final BoolQueryBuilder boolBuilder, DocumentMapper docMapper, final Set<String> fields, final MoreLikeThisRequest request) {
        if (getResponse.isSourceEmpty()) {
            return;
        }
        docMapper.parse(SourceToParse.source(getResponse.getSourceAsBytesRef()).type(request.type()).id(request.id()), new DocumentMapper.ParseListenerAdapter() {
            @Override
            public boolean beforeFieldAdded(FieldMapper fieldMapper, Field field, Object parseContext) {
                if (field.fieldType().indexOptions() == IndexOptions.NONE) {
                    return false;
                }
                if (fieldMapper instanceof InternalMapper) {
                    return true;
                }
                String value = fieldMapper.value(convertField(field)).toString();
                if (value == null) {
                    return false;
                }

                if (fields.isEmpty() || fields.contains(field.name())) {
                    addMoreLikeThis(request, boolBuilder, fieldMapper, field, !fields.isEmpty());
                }

                return false;
            }
        });
    }

    private Object convertField(Field field) {
        if (field.stringValue() != null) {
            return field.stringValue();
        } else if (field.binaryValue() != null) {
            return BytesRef.deepCopyOf(field.binaryValue()).bytes;
        } else if (field.numericValue() != null) {
            return field.numericValue();
        } else {
            throw new ElasticsearchIllegalStateException("Field should have either a string, numeric or binary value");
        }
    }

    private void addMoreLikeThis(MoreLikeThisRequest request, BoolQueryBuilder boolBuilder, FieldMapper fieldMapper, Field field, boolean failOnUnsupportedField) {
        addMoreLikeThis(request, boolBuilder, field.name(), fieldMapper.value(convertField(field)).toString(), failOnUnsupportedField);
    }

    private void addMoreLikeThis(MoreLikeThisRequest request, BoolQueryBuilder boolBuilder, String fieldName, String likeText, boolean failOnUnsupportedField) {
        MoreLikeThisQueryBuilder mlt = moreLikeThisQuery(fieldName)
                .likeText(likeText)
                .minimumShouldMatch(request.minimumShouldMatch())
                .boostTerms(request.boostTerms())
                .minDocFreq(request.minDocFreq())
                .maxDocFreq(request.maxDocFreq())
                .minWordLength(request.minWordLength())
                .maxWordLength(request.maxWordLength())
                .minTermFreq(request.minTermFreq())
                .maxQueryTerms(request.maxQueryTerms())
                .stopWords(request.stopWords())
                .failOnUnsupportedField(failOnUnsupportedField);
        boolBuilder.should(mlt);
    }
}
