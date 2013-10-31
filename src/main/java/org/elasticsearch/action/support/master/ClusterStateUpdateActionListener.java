/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionListener;

/**
 * Listener used for cluster state updates processing, supports acknowledgement logic
 * Base implementation used when applying cluster state updates from a transport action
 */
public abstract class ClusterStateUpdateActionListener<UpdateResponse extends ClusterStateUpdateResponse, Response extends AcknowledgedResponse> implements ClusterStateUpdateListener<UpdateResponse> {

    private final ActionListener<Response> actionListener;

    protected ClusterStateUpdateActionListener(ActionListener<Response> actionListener) {
        this.actionListener = actionListener;
    }

    /**
     * Translates the response obtained from the cluster state update
     * to the one to be returned via java api
     */
    protected abstract Response newResponse(UpdateResponse clusterStateUpdateResponse);

    @Override
    public void onResponse(UpdateResponse response) {
        actionListener.onResponse(newResponse(response));
    }

    @Override
    public void onFailure(Throwable t) {
        actionListener.onFailure(t);
    }
}
