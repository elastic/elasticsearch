/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.transport.TransportRequest;

/**
 * This class analyzes an incoming request and its action name, and returns the security action name for it.
 * In many cases the action name is the same as the original one used in es core, but in some exceptional cases it might need
 * to be converted. For instance a clear_scroll that targets all opened scrolls gets converted to a different action that requires
 * cluster privileges instead of the default indices privileges, still valid for clear scrolls that target specific scroll ids.
 */
public class SecurityActionMapper {

    static final String CLUSTER_PERMISSION_SCROLL_CLEAR_ALL_NAME = "cluster:admin/indices/scroll/clear_all";
    static final String CLUSTER_PERMISSION_ANALYZE = "cluster:admin/analyze";
    static final String CLUSTER_PERMISSION_PAINLESS_EXECUTE = "cluster:admin/scripts/painless/execute";

    /**
     * Returns the security specific action name given the incoming action name and request
     */
    public static String action(String action, TransportRequest request) {
        switch (action) {
            case ClearScrollAction.NAME -> {
                assert request instanceof ClearScrollRequest;
                boolean isClearAllScrollRequest = ((ClearScrollRequest) request).scrollIds().contains("_all");
                if (isClearAllScrollRequest) {
                    return CLUSTER_PERMISSION_SCROLL_CLEAR_ALL_NAME;
                }
            }
            case AnalyzeAction.NAME, AnalyzeAction.NAME + "[s]" -> {
                assert request instanceof AnalyzeAction.Request;
                String[] indices = ((AnalyzeAction.Request) request).indices();
                if (indices == null || (indices.length == 1 && indices[0] == null)) {
                    return CLUSTER_PERMISSION_ANALYZE;
                }
            }
            case CLUSTER_PERMISSION_PAINLESS_EXECUTE, CLUSTER_PERMISSION_PAINLESS_EXECUTE + "[s]" -> {
                assert request instanceof SingleShardRequest;
                String index = ((SingleShardRequest<?>) request).index();
                if (index != null) {
                    return action.replace("cluster:admin", "indices:data/read");
                }
            }
        }
        return action;
    }
}
