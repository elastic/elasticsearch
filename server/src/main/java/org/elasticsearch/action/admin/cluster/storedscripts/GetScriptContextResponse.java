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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptContextInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class GetScriptContextResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField CONTEXTS = new ParseField("contexts");
    final Set<ScriptContextInfo> contexts;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetScriptContextResponse,Void> PARSER =
        new ConstructingObjectParser<>("get_script_context", true,
            (a) -> {
                    return new GetScriptContextResponse((List<ScriptContextInfo>)a[0]);
            }
        );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (parser, ctx) -> ScriptContextInfo.PARSER.apply(parser, ctx), CONTEXTS);
    }

    GetScriptContextResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readInt();
        HashSet<ScriptContextInfo> contexts = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            contexts.add(new ScriptContextInfo(in));
        }
        this.contexts = Collections.unmodifiableSet(contexts);
    }

    GetScriptContextResponse(Collection<ScriptContextInfo> contexts) {
        this.contexts = Set.copyOf(contexts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.contexts.size());
        for (ScriptContextInfo context: this.contexts) {
            context.writeTo(out);
        }
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startArray(CONTEXTS.getPreferredName());
        for (ScriptContextInfo context: contexts.stream().sorted(Comparator.comparing(s -> s.name)).collect(Collectors.toList())) {
            context.toXContent(builder, params);
        }
        builder.endArray().endObject(); // CONTEXTS
        return builder;
    }

    public static GetScriptContextResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetScriptContextResponse that = (GetScriptContextResponse) o;
        return new HashSet<>(contexts).equals(new HashSet<>(that.contexts));
    }

    @Override
    public int hashCode() {
        return Objects.hash(new HashSet<>(contexts));
    }
}
