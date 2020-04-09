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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.eql.session.Configuration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Collections;
import java.util.List;

public class Querier {

    private static final Logger log = LogManager.getLogger(Querier.class);

    private final Configuration cfg;
    private final Client client;
    private final TimeValue keepAlive;
    private final QueryBuilder filter;


    public Querier(EqlSession eqlSession) {
        this.cfg = eqlSession.configuration();
        this.client = eqlSession.client();
        this.keepAlive = cfg.requestTimeout();
        this.filter = cfg.filter();
    }


    public void query(List<Attribute> output, QueryContainer container, String index, ActionListener<Results> listener) {
        // prepare the request
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, cfg.size());

        // set query timeout
        sourceBuilder.timeout(cfg.requestTimeout());

        if (log.isTraceEnabled()) {
            log.trace("About to execute query {} on {}", StringUtils.toString(sourceBuilder), index);
        }
        
        SearchRequest search = prepareRequest(client, sourceBuilder, cfg.requestTimeout(), false,
                Strings.commaDelimitedListToStringArray(index));

        ActionListener<SearchResponse> l = new SearchAfterListener(listener, client, cfg, output, container, search);

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

    protected static void logSearchResponse(SearchResponse response, Logger logger) {
        List<Aggregation> aggs = Collections.emptyList();
        if (response.getAggregations() != null) {
            aggs = response.getAggregations().asList();
        }
        StringBuilder aggsNames = new StringBuilder();
        for (int i = 0; i < aggs.size(); i++) {
            aggsNames.append(aggs.get(i).getName() + (i + 1 == aggs.size() ? "" : ", "));
        }

        logger.trace("Got search response [hits {} {}, {} aggregations: [{}], {} failed shards, {} skipped shards, "
                + "{} successful shards, {} total shards, took {}, timed out [{}]]", response.getHits().getTotalHits().relation.toString(),
                response.getHits().getTotalHits().value, aggs.size(), aggsNames, response.getFailedShards(), response.getSkippedShards(),
                response.getSuccessfulShards(), response.getTotalShards(), response.getTook(), response.isTimedOut());
    }
}