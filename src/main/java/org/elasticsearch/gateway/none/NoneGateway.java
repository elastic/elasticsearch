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

package org.elasticsearch.gateway.none;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.gateway.none.NoneIndexGatewayModule;

/**
 *
 */
public class NoneGateway extends AbstractLifecycleComponent<Gateway> implements Gateway {

    public static final String TYPE = "none";

    private final ClusterName clusterName;

    @Inject
    public NoneGateway(Settings settings, ClusterName clusterName) {
        super(settings);
        this.clusterName = clusterName;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String toString() {
        return "_none_";
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public void performStateRecovery(GatewayStateRecoveredListener listener) throws GatewayException {
        logger.debug("performing state recovery");
        listener.onSuccess(ClusterState.builder(clusterName).build());
    }

    @Override
    public Class<? extends Module> suggestIndexGateway() {
        return NoneIndexGatewayModule.class;
    }

    @Override
    public void reset() {
    }
}
