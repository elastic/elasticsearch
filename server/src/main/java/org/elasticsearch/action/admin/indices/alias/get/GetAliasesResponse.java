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

package org.elasticsearch.action.admin.indices.alias.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetAliasesResponse extends ActionResponse {

    private ImmutableOpenMap<String, List<AliasMetaData>> aliases = ImmutableOpenMap.of();

    public GetAliasesResponse(ImmutableOpenMap<String, List<AliasMetaData>> aliases) {
        this.aliases = aliases;
    }

    public GetAliasesResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            List<AliasMetaData> value = new ArrayList<>(valueSize);
            for (int j = 0; j < valueSize; j++) {
                value.add(new AliasMetaData(in));
            }
            aliasesBuilder.put(key, Collections.unmodifiableList(value));
        }
        aliases = aliasesBuilder.build();
    }

    public ImmutableOpenMap<String, List<AliasMetaData>> getAliases() {
        return aliases;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(aliases.size());
        for (ObjectObjectCursor<String, List<AliasMetaData>> entry : aliases) {
            out.writeString(entry.key);
            out.writeVInt(entry.value.size());
            for (AliasMetaData aliasMetaData : entry.value) {
                aliasMetaData.writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetAliasesResponse that = (GetAliasesResponse) o;
        return Objects.equals(aliases, that.aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases);
    }
}
