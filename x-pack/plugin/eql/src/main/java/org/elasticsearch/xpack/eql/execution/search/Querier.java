/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.eql.execution.listener.BasicListener;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.util.StringUtils;

public class Querier {

    private static final Logger log = LogManager.getLogger(Querier.class);

    private final EqlConfiguration cfg;
    private final Client client;
    private final TimeValue keepAlive;
    private final QueryBuilder filter;


    public Querier(EqlSession eqlSession) {
        this.cfg = eqlSession.configuration();
        this.client = eqlSession.client();
        this.keepAlive = cfg.requestTimeout();
        this.filter = cfg.filter();
    }


    public void query(QueryContainer container, String index, ActionListener<Results> listener) {
        // prepare the request
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, cfg.size());

        // set query timeout
        sourceBuilder.timeout(cfg.requestTimeout());

        if (log.isTraceEnabled()) {
            log.trace("About to execute query {} on {}", StringUtils.toString(sourceBuilder), index);
        }
        if (cfg.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }
        SearchRequest search = prepareRequest(client, sourceBuilder, cfg.requestTimeout(), false,
                Strings.commaDelimitedListToStringArray(index));

        ActionListener<SearchResponse> l = new BasicListener(listener, search);

        client.search(search, l);
    }

    public static SearchRequest prepareRequest(Client client, SearchSourceBuilder source, TimeValue timeout, boolean includeFrozen,
                                               String... indices) {
        return client.prepareSearch(indices)
                // always track total hits accurately
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(false)
                .setSource(source)
                .setTimeout(timeout)
                .setIndicesOptions(
                        includeFrozen ? IndexResolver.FIELD_CAPS_FROZEN_INDICES_OPTIONS : IndexResolver.FIELD_CAPS_INDICES_OPTIONS)
                .request();
    }
}
