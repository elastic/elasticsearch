/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;

/**
 * Invokes a Close Index Step on a index.
 */
public class CloseIndexStep extends AsyncActionStep {
    public static final String NAME = "close-index";

    CloseIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState,
                              ClusterStateObserver observer, Listener listener) {
        if(indexMetaData.getState() == IndexMetaData.State.OPEN) {
            CloseIndexRequest request = new CloseIndexRequest(indexMetaData.getIndex().getName());
            getClient().admin().indices()
                .close(request, ActionListener.wrap(closeIndexResponse -> listener.onResponse(true), listener::onFailure));
        }
        else {
            listener.onResponse(true);
        }
    }


}
