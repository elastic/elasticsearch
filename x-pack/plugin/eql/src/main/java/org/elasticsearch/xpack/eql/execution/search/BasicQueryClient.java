/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.ql.util.ActionListeners;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @Override
    public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
        IdsQueryBuilder idsQuery = idsQuery();

        int innerListSize = 0;
        Set<String> indices = new HashSet<>();

        // associate each reference with its own
        final Map<HitReference, List<Integer>> referenceToPosition = new HashMap<>();
        int counter = 0;

        for (List<HitReference> list : refs) {
            innerListSize = list.size();
            for (HitReference ref : list) {
                idsQuery.addIds(ref.id());
                indices.add(ref.index());
                // remember the reference position
                List<Integer> positions = referenceToPosition.computeIfAbsent(ref, v -> new ArrayList<>(1));
                positions.add(counter++);
            }
        }

        SearchSourceBuilder builder = SearchSourceBuilder.searchSource()
            // make sure to fetch the whole source
            .fetchSource(FetchSourceContext.FETCH_SOURCE)
            .trackTotalHits(false)
            .trackScores(false)
            .query(idsQuery);

        final int listSize = innerListSize;
        final int topListSize = counter / listSize;
        // pre-allocate the response matrix
        @SuppressWarnings({"rawtypes", "unchecked"})
        List<SearchHit>[] hits = new List[topListSize];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = Arrays.asList(new SearchHit[listSize]);
        }
        final List<List<SearchHit>> seq = Arrays.asList(hits);

        SearchRequest search = prepareRequest(client, builder, false, indices.toArray(new String[0]));

        search(search, ActionListeners.map(listener, r -> {
            for (SearchHit hit : RuntimeUtils.searchHits(r)) {
                List<Integer> positions = referenceToPosition.get(new HitReference(hit));
                positions.forEach(pos -> seq.get(pos / listSize).set(pos % listSize, hit));
            }
            return seq;
        }));
    }
}
