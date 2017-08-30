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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;

public class GetStoredScriptResponse extends ActionResponse implements ToXContentObject {

    private StoredScriptSource source;

    GetStoredScriptResponse() {
    }

    GetStoredScriptResponse(StoredScriptSource source) {
        this.source = source;
    }

    /**
     * @return if a stored script and if not found <code>null</code>
     */
    public StoredScriptSource getSource() {
        return source;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        source.toXContent(builder, params);

        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.readBoolean()) {
            if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
                source = new StoredScriptSource(in);
            } else {
                source = new StoredScriptSource(in.readString());
            }
        } else {
            source = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (source == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);

            if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
                source.writeTo(out);
            } else {
                out.writeString(source.getSource());
            }
        }
    }
}
