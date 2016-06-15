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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.plugins.Plugin;

public class ReindexPlugin extends Plugin {
    public static final String NAME = "reindex";

    public void onModule(ActionModule actionModule) {
        actionModule.registerAction(ReindexAction.INSTANCE, TransportReindexAction.class);
        actionModule.registerAction(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class);
        actionModule.registerAction(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class);
        actionModule.registerAction(RethrottleAction.INSTANCE, TransportRethrottleAction.class);
    }

    public void onModule(NetworkModule networkModule) {
        networkModule.registerRestHandler(RestReindexAction.class);
        networkModule.registerRestHandler(RestUpdateByQueryAction.class);
        networkModule.registerRestHandler(RestDeleteByQueryAction.class);
        networkModule.registerRestHandler(RestRethrottleAction.class);
        networkModule.registerTaskStatus(BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new);
    }
}
