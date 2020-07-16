/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.prepareRequest;

public class BasicQueryClient implements QueryClient {

    private static final Logger log = RuntimeUtils.QUERY_LOG;

    private final EqlConfiguration cfg;
    private final Client client;
    private final String indices;

    public BasicQueryClient(EqlSession eqlSession) {
        this.cfg = eqlSession.configuration();
        this.client = eqlSession.client();
        this.indices = cfg.indexAsWildcard();
    }

    @Override
    public void query(QueryRequest request, ActionListener<Payload> listener) {
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
        client.search(search, new BasicListener(listener));
    }

    @Override
    public void get(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
        MultiGetRequestBuilder requestBuilder = client.prepareMultiGet();
        // no need for real-time
        requestBuilder.setRealtime(false)
                      .setRefresh(false);

        int sz = 0;

        for (List<HitReference> list : refs) {
            sz = list.size();
            for (HitReference ref : list) {
                Item item = new Item(ref.index(), ref.id());
                // make sure to get the whole source
                item.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);
                requestBuilder.add(item);
            }
        }
        
        final int listSize = sz;
        client.multiGet(requestBuilder.request(), wrap(r -> {
            List<List<SearchHit>> hits = new ArrayList<>(r.getResponses().length / listSize);
            
            List<SearchHit> sequence = new ArrayList<>(listSize);

            int counter = 0;
            for (MultiGetItemResponse mgr : r.getResponses()) {
                if (mgr.isFailed()) {
                    listener.onFailure(mgr.getFailure().getFailure());
                    return;
                }

                GetResponse response = mgr.getResponse();
                SearchHit hit = new SearchHit(-1, response.getId(), null, null);
                hit.sourceRef(response.getSourceInternal());
                // need to create these objects to set the index
                hit.shard(new SearchShardTarget(null, new ShardId(response.getIndex(), "", -1), null, null));
                hit.setSeqNo(response.getSeqNo());
                hit.setPrimaryTerm(response.getPrimaryTerm());
                hit.version(response.getVersion());


                sequence.add(hit);

                if (++counter == listSize) {
                    counter = 0;
                    hits.add(sequence);
                    sequence = new ArrayList<>(listSize);
                }
            }
            // send the results
            listener.onResponse(hits);

        }, listener::onFailure));
    }
}