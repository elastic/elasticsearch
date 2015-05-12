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

package org.elasticsearch.action.admin.indices.syncedflush;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indices.SyncedFlushService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A response to a synced flush action on several indices.
 */
public class SyncedFlushIndicesResponse extends ActionResponse {

    private Set<SyncedFlushService.SyncedFlushResult> results;

    SyncedFlushIndicesResponse() {
    }

    SyncedFlushIndicesResponse(Set<SyncedFlushService.SyncedFlushResult> results) {
        this.results = results;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        results = new HashSet<>();
        for (int i = 0; i< size; i++) {
            SyncedFlushService.SyncedFlushResult syncedFlushResult = new SyncedFlushService.SyncedFlushResult();
            syncedFlushResult.readFrom(in);
            results.add(syncedFlushResult);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(results.size());
        for (SyncedFlushService.SyncedFlushResult syncedFlushResult : results) {
            syncedFlushResult.writeTo(out);
        }
    }

    public Set<SyncedFlushService.SyncedFlushResult> results() {
        return results;
    }
}
