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

package org.elasticsearch.action.fieldstats;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FieldStatsShardResponse extends BroadcastShardResponse {

    private Map<String, FieldStats<?>> fieldStats;

    public FieldStatsShardResponse() {
    }

    public FieldStatsShardResponse(ShardId shardId, Map<String, FieldStats<?>> fieldStats) {
        super(shardId);
        this.fieldStats = fieldStats;
    }

    public Map<String, FieldStats<?>> getFieldStats() {
        return fieldStats;
    }

    Map<String, FieldStats<?> > filterNullMinMax() {
        return fieldStats.entrySet().stream()
            .filter((e) -> e.getValue().hasMinMax())
            .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        final int size = in.readVInt();
        fieldStats = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            FieldStats value = FieldStats.readFrom(in);
            fieldStats.put(key, value);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        final Map<String, FieldStats<?> > stats;
        if (out.getVersion().before(Version.V_5_2_0_UNRELEASED)) {
            /**
             * FieldStats with null min/max are not (de)serializable in versions prior to {@link Version.V_5_2_0_UNRELEASED}
             */
            stats = filterNullMinMax();
        } else {
            stats = getFieldStats();
        }
        out.writeVInt(stats.size());
        for (Map.Entry<String, FieldStats<?>> entry : stats.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }
}
