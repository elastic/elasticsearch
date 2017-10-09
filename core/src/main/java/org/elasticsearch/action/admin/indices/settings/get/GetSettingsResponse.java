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

package org.elasticsearch.action.admin.indices.settings.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

public class GetSettingsResponse extends ActionResponse {

    private ImmutableOpenMap<String, Settings> indexToSettings = ImmutableOpenMap.of();

    public GetSettingsResponse(ImmutableOpenMap<String, Settings> indexToSettings) {
        this.indexToSettings = indexToSettings;
    }

    GetSettingsResponse() {
    }

    public ImmutableOpenMap<String, Settings> getIndexToSettings() {
        return indexToSettings;
    }

    public String getSetting(String index, String setting) {
        Settings settings = indexToSettings.get(index);
        if (setting != null) {
            return settings.get(setting);
        } else {
            return null;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, Settings> builder = ImmutableOpenMap.builder();
        for (int i = 0; i < size; i++) {
            builder.put(in.readString(), Settings.readSettingsFromStream(in));
        }
        indexToSettings = builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indexToSettings.size());
        for (ObjectObjectCursor<String, Settings> cursor : indexToSettings) {
            out.writeString(cursor.key);
            Settings.writeSettingsToStream(cursor.value, out);
        }
    }
}
