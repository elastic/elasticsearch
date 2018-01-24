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

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetStoredScriptsResponse extends ActionResponse implements ToXContentObject {

    private Map<String, StoredScriptSource> storedScripts;

    GetStoredScriptsResponse() {
    }

    GetStoredScriptsResponse(Map<String, StoredScriptSource> storedScripts) {
        this.storedScripts = storedScripts;
    }

    public Map<String, StoredScriptSource> getStoredScripts() {
        return storedScripts;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, StoredScriptSource> storedScript : getStoredScripts().entrySet()) {

            builder.startObject(storedScript.getKey());

            builder.startObject();
            storedScript.getValue().toXContent(builder, params);
            builder.endObject();

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        int size = in.readVInt();
        storedScripts = new HashMap<>(size);
        for (int i = 0 ; i < size ; i++) {
            String id = in.readString();
            storedScripts.put(id, new StoredScriptSource(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(storedScripts.size());
        for (Map.Entry<String, StoredScriptSource> storedScript : storedScripts.entrySet()) {
            out.writeString(storedScript.getKey());
            storedScript.getValue().writeTo(out);
        }
    }
}
