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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptLanguagesInfo;

import java.io.IOException;
import java.util.Objects;

public class GetScriptLanguageResponse extends ActionResponse implements StatusToXContentObject, Writeable {
    public final ScriptLanguagesInfo info;

    GetScriptLanguageResponse(ScriptLanguagesInfo info) {
        this.info = info;
    }

    GetScriptLanguageResponse(StreamInput in) throws IOException {
        super(in);
        info = new ScriptLanguagesInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    public static GetScriptLanguageResponse fromXContent(XContentParser parser) throws IOException {
        return new GetScriptLanguageResponse(ScriptLanguagesInfo.fromXContent(parser));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GetScriptLanguageResponse that = (GetScriptLanguageResponse) o;
        return info.equals(that.info);
    }

    @Override
    public int hashCode() { return Objects.hash(info); }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return info.toXContent(builder, params);
    }
}
