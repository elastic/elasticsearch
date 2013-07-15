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

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class GetAliasesResponse extends ActionResponse {

    private Map<String, List<AliasMetaData>> aliases = new HashMap<String, List<AliasMetaData>>();

    public GetAliasesResponse(Map<String, List<AliasMetaData>> aliases) {
        this.aliases = aliases;
    }

    GetAliasesResponse() {
    }


    public Map<String, List<AliasMetaData>> getAliases() {
        return aliases;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            List<AliasMetaData> value = new ArrayList<AliasMetaData>(valueSize);
            for (int j = 0; j < valueSize; j++) {
                value.add(AliasMetaData.Builder.readFrom(in));
            }
            aliases.put(key, value);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(aliases.size());
        for (Map.Entry<String, List<AliasMetaData>> entry : aliases.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (AliasMetaData aliasMetaData : entry.getValue()) {
                AliasMetaData.Builder.writeTo(aliasMetaData, out);
            }
        }
    }
}
