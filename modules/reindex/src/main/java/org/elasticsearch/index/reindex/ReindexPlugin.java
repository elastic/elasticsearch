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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.List;

public class ReindexPlugin extends Plugin implements ActionPlugin {
    public static final String NAME = "reindex";

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(ReindexAction.INSTANCE, TransportReindexAction.class),
                new ActionHandler<>(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class),
                new ActionHandler<>(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class),
                new ActionHandler<>(RethrottleAction.INSTANCE, TransportRethrottleAction.class));
    }

    public void onModule(NetworkModule networkModule) {
        networkModule.registerRestHandler(RestReindexAction.class);
        networkModule.registerRestHandler(RestUpdateByQueryAction.class);
        networkModule.registerRestHandler(RestDeleteByQueryAction.class);
        networkModule.registerRestHandler(RestRethrottleAction.class);
        networkModule.registerTaskStatus(BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new);
    }
}
