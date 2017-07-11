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

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 * A base class for read operations that needs to be performed on the master node.
 * Can also be executed on the local node if needed.
 */
public abstract class TransportMasterNodeReadAction<Request extends MasterNodeReadRequest<Request>, Response extends ActionResponse>
        extends TransportMasterNodeAction<Request, Response> {

    public static final Setting<Boolean> FORCE_LOCAL_SETTING =
        Setting.boolSetting("action.master.force_local", false, Property.NodeScope);

    private final boolean forceLocal;

    protected TransportMasterNodeReadAction(Settings settings, String actionName, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        this(settings, actionName, true, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,request);
    }

    protected TransportMasterNodeReadAction(Settings settings, String actionName, boolean checkSizeLimit, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        super(settings, actionName, checkSizeLimit, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver,request);
        this.forceLocal = FORCE_LOCAL_SETTING.get(settings);
    }

    @Override
    protected final boolean localExecute(Request request) {
        return forceLocal || request.local();
    }
}
