/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.ql.util.StringUtils;

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
}
