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
package org.elasticsearch.percolator;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportPercolateAction extends HandledTransportAction<PercolateRequest, PercolateResponse> {

    private final Client client;
    private final ParseFieldMatcher parseFieldMatcher;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggParsers;

    @Inject
    public TransportPercolateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                    Client client, IndicesQueriesRegistry indicesQueriesRegistry, AggregatorParsers aggParsers) {
        super(settings, PercolateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, PercolateRequest::new);
        this.client = client;
        this.aggParsers = aggParsers;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.queryRegistry = indicesQueriesRegistry;
    }

    @Override
    protected void doExecute(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        if (request.getRequest() != null) {
            client.get(request.getRequest(), new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse getResponse) {
                    if (getResponse.isExists()) {
                        innerDoExecute(request, getResponse.getSourceAsBytesRef(), listener);
                    } else {
                        onFailure(new ResourceNotFoundException("percolate document [{}/{}/{}] doesn't exist", request.getRequest().index(), request.getRequest().type(), request.getRequest().id()));
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            innerDoExecute(request, null, listener);
        }
    }

    private void innerDoExecute(PercolateRequest request, BytesReference docSource, ActionListener<PercolateResponse> listener) {
        SearchRequest searchRequest;
        try {
            searchRequest = createSearchRequest(request, docSource, queryRegistry, aggParsers, parseFieldMatcher);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }
        client.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    listener.onResponse(createPercolateResponse(searchResponse, request.onlyCount()));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    public static SearchRequest createSearchRequest(PercolateRequest percolateRequest, BytesReference documentSource, IndicesQueriesRegistry queryRegistry, AggregatorParsers aggParsers, ParseFieldMatcher parseFieldMatcher) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        if (percolateRequest.indices() != null) {
            searchRequest.indices(percolateRequest.indices());
        }
        searchRequest.indicesOptions(percolateRequest.indicesOptions());
        searchRequest.routing(percolateRequest.routing());
        searchRequest.preference(percolateRequest.preference());

        BytesReference querySource = null;
        XContentBuilder searchSource = XContentFactory.jsonBuilder().startObject();
        if (percolateRequest.source() != null && percolateRequest.source().length() > 0) {
            try (XContentParser parser = XContentHelper.createParser(percolateRequest.source())) {
                String currentFieldName = null;
                XContentParser.Token token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException("Unknown token [" + token+ "]");
                }

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("doc".equals(currentFieldName)) {
                            XContentBuilder builder = XContentFactory.jsonBuilder();
                            builder.copyCurrentStructure(parser);
                            builder.flush();
                            documentSource = builder.bytes();
                        } else if ("query".equals(currentFieldName) || "filter".equals(currentFieldName)) {
                            XContentBuilder builder = XContentFactory.jsonBuilder();
                            builder.copyCurrentStructure(parser);
                            builder.flush();
                            querySource = builder.bytes();
                        } else if ("sort".equals(currentFieldName)) {
                            searchSource.field("sort");
                            searchSource.copyCurrentStructure(parser);
                        } else if ("aggregations".equals(currentFieldName)) {
                            searchSource.field("aggregations");
                            searchSource.copyCurrentStructure(parser);
                        } else if ("highlight".equals(currentFieldName)) {
                            searchSource.field("highlight");
                            searchSource.copyCurrentStructure(parser);
                        } else {
                            throw new IllegalArgumentException("Unknown field [" + currentFieldName+ "]");
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if ("sort".equals(currentFieldName)) {
                            searchSource.field("sort");
                            searchSource.copyCurrentStructure(parser);
                        } else {
                            throw new IllegalArgumentException("Unknown field [" + currentFieldName+ "]");
                        }
                    } else if (token.isValue()) {
                        if ("size".equals(currentFieldName)) {
                            searchSource.field("size", parser.intValue());
                        } else if ("sort".equals(currentFieldName)) {
                            searchSource.field("sort", parser.text());
                        } else if ("track_scores".equals(currentFieldName) || "trackScores".equals(currentFieldName)) {
                            searchSource.field("track_scores", parser.booleanValue());
                        } else {
                            throw new IllegalArgumentException("Unknown field [" + currentFieldName+ "]");
                        }
                    } else {
                        throw new IllegalArgumentException("Unknown token [" + token + "]");
                    }
                }
            }
        }

        if (percolateRequest.onlyCount()) {
            searchSource.field("size", 0);
        }

        PercolateQueryBuilder percolateQueryBuilder =
                new PercolateQueryBuilder("query", percolateRequest.documentType(), documentSource);
        if (querySource != null) {
            try (XContentParser parser = XContentHelper.createParser(querySource)) {
                QueryParseContext queryParseContext = new QueryParseContext(queryRegistry, parser, parseFieldMatcher);
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                queryParseContext.parseInnerQueryBuilder().ifPresent(boolQueryBuilder::must);
                boolQueryBuilder.filter(percolateQueryBuilder);
                searchSource.field("query", boolQueryBuilder);
            }
        } else {
            // wrapping in a constant score query with boost 0 for bwc reason.
            // percolator api didn't emit scores before and never included scores
            // for how well percolator queries matched with the document being percolated
            searchSource.field("query", new ConstantScoreQueryBuilder(percolateQueryBuilder).boost(0f));
        }

        searchSource.endObject();
        searchSource.flush();
        BytesReference source = searchSource.bytes();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(source)) {
            QueryParseContext context = new QueryParseContext(queryRegistry, parser, parseFieldMatcher);
            searchSourceBuilder.parseXContent(context, aggParsers, null);
            searchRequest.source(searchSourceBuilder);
            return searchRequest;
        }
    }

    public static PercolateResponse createPercolateResponse(SearchResponse searchResponse, boolean onlyCount) {
        SearchHits hits = searchResponse.getHits();
        PercolateResponse.Match[] matches;
        if (onlyCount) {
            matches = null;
        } else {
            matches = new PercolateResponse.Match[hits.getHits().length];
            for (int i = 0; i < hits.getHits().length; i++) {
                SearchHit hit = hits.getHits()[i];
                matches[i] = new PercolateResponse.Match(new Text(hit.getIndex()), new Text(hit.getId()), hit.getScore(), hit.getHighlightFields());
            }
        }

        List<ShardOperationFailedException> shardFailures = new ArrayList<>(searchResponse.getShardFailures().length);
        for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
            shardFailures.add(new DefaultShardOperationFailedException(shardSearchFailure.index(), shardSearchFailure.shardId(),
                    shardSearchFailure.getCause()));
        }

        return new PercolateResponse(
            searchResponse.getTotalShards(), searchResponse.getSuccessfulShards(), searchResponse.getFailedShards(),
            shardFailures, matches, hits.getTotalHits(), searchResponse.getTookInMillis(), (InternalAggregations) searchResponse.getAggregations()
        );
    }

}
