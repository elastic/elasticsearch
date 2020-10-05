/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.prepareRequest;

public class BasicQueryClient implements QueryClient {

    private static final Logger log = RuntimeUtils.QUERY_LOG;

    private final EqlConfiguration cfg;
    final Client client;
    final String[] indices;

    public BasicQueryClient(EqlSession eqlSession) {
        this.cfg = eqlSession.configuration();
        this.client = eqlSession.client();
        this.indices = cfg.indices();
    }

    @Override
    public void query(QueryRequest request, ActionListener<SearchResponse> listener) {
        SearchSourceBuilder searchSource = request.searchSource();
        // set query timeout
        searchSource.timeout(cfg.requestTimeout());

        if (log.isTraceEnabled()) {
            log.trace("About to execute query {} on {}", StringUtils.toString(searchSource), indices);
        }
        if (cfg.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        SearchRequest search = prepareRequest(client, searchSource, false, indices);
        search(search, new BasicListener(listener));
    }

    protected void search(SearchRequest search, ActionListener<SearchResponse> listener) {
        client.search(search, listener);
    }

    protected void search(MultiSearchRequest search, ActionListener<MultiSearchResponse> listener) {
        client.multiSearch(search, listener);
    }

    @Override
    public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
        int innerListSize = 0;

        Map<String, IdsQueryBuilder> queries = new HashMap<>();

        // associate each reference with its positions inside the matrix
        final Map<HitReference, List<Integer>> referenceToPosition = new HashMap<>();
        int counter = 0;

        for (List<HitReference> list : refs) {
            innerListSize = list.size();
            for (HitReference ref : list) {
                // keep ids per index
                IdsQueryBuilder query = queries.computeIfAbsent(ref.index(), v -> idsQuery());
                query.addIds(ref.id());
                // save the ref position inside the matrix
                List<Integer> positions = referenceToPosition.computeIfAbsent(ref, v -> new ArrayList<>(1));
                positions.add(counter++);
            }
        }

        final int listSize = innerListSize;
        final int topListSize = counter / listSize;

        // pre-allocate the response matrix
        @SuppressWarnings({"rawtypes", "unchecked"})
        List<SearchHit>[] hits = new List[topListSize];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = Arrays.asList(new SearchHit[listSize]);
        }
        final List<List<SearchHit>> seq = Arrays.asList(hits);

        // create a multi-search
        MultiSearchRequestBuilder multiSearchBuilder = client.prepareMultiSearch();
        for (Map.Entry<String, IdsQueryBuilder> entry : queries.entrySet()) {
            IdsQueryBuilder idQuery = entry.getValue();
            SearchSourceBuilder builder = SearchSourceBuilder.searchSource()
                // make sure to fetch the whole source
                .fetchSource(FetchSourceContext.FETCH_SOURCE)
                .trackTotalHits(false)
                .trackScores(false)
                .query(idQuery)
                // the default size is 10 so be sure to change it
                // NB:this is different from mget
                .size(idQuery.ids().size());

            SearchRequest search = prepareRequest(client, builder, false, entry.getKey());
            multiSearchBuilder.add(search);
        }

        search(multiSearchBuilder.request(), ActionListener.wrap(r -> {
            for (MultiSearchResponse.Item item : r.getResponses()) {
                // check for failures
                if (item.isFailure()) {
                    listener.onFailure(item.getFailure());
                    return;
                }
                // otherwise proceed
                List<SearchHit> docs = RuntimeUtils.searchHits(item.getResponse());
                // for each doc, find its reference and its position inside the matrix
                for (SearchHit doc : docs) {
                    HitReference docRef = new HitReference(doc);
                    List<Integer> positions = referenceToPosition.get(docRef);
                    positions.forEach(pos -> {
                        SearchHit previous = seq.get(pos / listSize).set(pos % listSize, doc);
                        if (previous != null) {
                            throw new EqlIllegalArgumentException("Overriding sequence match [{}] with [{}]",
                                new HitReference(previous), docRef);
                        }
                    });
                }
            }
            listener.onResponse(seq);
        }, listener::onFailure));
    }
}
