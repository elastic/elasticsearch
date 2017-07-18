/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.session.RowSetCursor;

import java.util.TimeZone;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.util.ActionUtils.chain;

public class TransportSqlAction extends HandledTransportAction<SqlRequest, SqlResponse> {

    //TODO: externalize timeout
    private final Cache<String, RowSetCursor> SESSIONS = CacheBuilder.<String, RowSetCursor> builder()
            .setMaximumWeight(1024)
            .setExpireAfterAccess(TimeValue.timeValueMinutes(10))
            .setExpireAfterWrite(TimeValue.timeValueMinutes(10))
            .build();
    
    private final Supplier<String> ephemeralId;
    private final PlanExecutor planExecutor;

    @Inject
    public TransportSqlAction(Settings settings, ThreadPool threadPool,
            TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ClusterService clusterService,
            PlanExecutor planExecutor) {
        super(settings, SqlAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SqlRequest::new);

        this.planExecutor = planExecutor;
        // lazy init of the resolver
        ((EsCatalog) planExecutor.catalog()).setIndexNameExpressionResolver(indexNameExpressionResolver);

        ephemeralId = () -> transportService.getLocalNode().getEphemeralId();
    }

    @Override
    protected void doExecute(SqlRequest request, ActionListener<SqlResponse> listener) {
        String sessionId = request.sessionId();
        String query = request.query();
        TimeZone timeZone = request.timeZone();
        
        try {
            if (sessionId == null) {
                if (!Strings.hasText(query)) {
                    listener.onFailure(new SqlIllegalArgumentException("No query is given and request not part of a session"));
                    return;
                }

                planExecutor.sql(query, timeZone, chain(listener, c -> {
                            String id = generateId();
                            SESSIONS.put(id, c);
                            return new SqlResponse(id, c);
                        }));
            }
            else {
                RowSetCursor cursor = SESSIONS.get(sessionId);
                if (cursor == null) {
                    listener.onFailure(new SqlIllegalArgumentException("SQL session cannot be found"));
                }
                else {
                    cursor.nextSet(chain(listener, c -> {
                        return new SqlResponse(sessionId, c);
                    }));
                }
            }
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    private String generateId() {
        return ephemeralId.get() + "-" + UUIDs.base64UUID();
    }
}