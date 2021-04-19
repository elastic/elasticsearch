/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;

public class TransportMasterNodeActionUtils {

    /**
     * Allows to directly call
     * {@link TransportMasterNodeAction#masterOperation(org.elasticsearch.tasks.Task,MasterNodeRequest, ClusterState, ActionListener)}
     * which is a protected method.
     */
    public static <Request extends MasterNodeRequest<Request>, Response extends ActionResponse> void runMasterOperation(
        TransportMasterNodeAction<Request, Response> masterNodeAction, Request request, ClusterState clusterState,
        ActionListener<Response> actionListener) throws Exception {
        assert masterNodeAction.checkBlock(request, clusterState) == null;
        // TODO: pass through task here?
        masterNodeAction.masterOperation(null, request, clusterState, actionListener);
    }
}
