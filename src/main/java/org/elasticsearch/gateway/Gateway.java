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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;

/**
 *
 */
public interface Gateway extends LifecycleComponent<Gateway> {

    String type();

    void performStateRecovery(GatewayStateRecoveredListener listener) throws GatewayException;

    Class<? extends Module> suggestIndexGateway();

    void reset() throws Exception;

    interface GatewayStateRecoveredListener {
        void onSuccess(ClusterState recoveredState);

        void onFailure(String message);
    }
}
