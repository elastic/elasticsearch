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
package org.elasticsearch.indices.syncedflush;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ExecutionException;

public class SyncedFlushService extends AbstractComponent {

    private final TransportPreSyncedFlushAction transportPreSyncedFlushAction;
    private final TransportSyncedFlushAction transportSyncedFlushAction;

    @Inject
    public SyncedFlushService(Settings settings, TransportPreSyncedFlushAction transportPreSyncedFlushAction, TransportSyncedFlushAction transportSyncedFlushAction) {
        super(settings);
        this.transportPreSyncedFlushAction = transportPreSyncedFlushAction;
        this.transportSyncedFlushAction = transportSyncedFlushAction;
    }

    public SyncedFlushResponse attemptSyncedFlush(ShardId shardId) throws ExecutionException, InterruptedException {
        PreSyncedFlushResponse preSyncedFlushResponse = transportPreSyncedFlushAction.execute(new PreSyncedFlushRequest(shardId)).get();
        // exit if this did not work
        String syncId = Strings.base64UUID();
        SyncedFlushResponse syncedFlushResponse = transportSyncedFlushAction.execute(new SyncedFlushRequest(shardId, syncId, preSyncedFlushResponse.commitIds())).get();
        return syncedFlushResponse;
    }
}
