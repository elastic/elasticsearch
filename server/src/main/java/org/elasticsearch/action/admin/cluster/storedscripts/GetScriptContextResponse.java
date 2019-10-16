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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;

public class GetScriptContextResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField CONTEXTS = new ParseField("contexts");
    private final List<String> contextNames;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetScriptContextResponse,Void> PARSER =
        new ConstructingObjectParser<>("get_script_context", true,
            (a) -> {
                Map<String, Object> contexts = ((List<String>) a[0]).stream().collect(Collectors.toMap(
                    name -> name, name -> new Object()
                ));
                return new GetScriptContextResponse(contexts);
            }
        );

    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) ->
            {
                // advance empty object
                assert(p.nextToken() == START_OBJECT);
                assert(p.nextToken() == END_OBJECT);
                return n;
            },
            CONTEXTS
        );
    }

    GetScriptContextResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readInt();
        ArrayList<String> contextNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            contextNames.add(in.readString());
        }
        this.contextNames = Collections.unmodifiableList(contextNames);
    }

    GetScriptContextResponse(Map<String,Object> contexts) {
        List<String> contextNames = new ArrayList<>(contexts.keySet());
        contextNames.sort(String::compareTo);
        this.contextNames = Collections.unmodifiableList(contextNames);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.contextNames.size());
        for (String context: this.contextNames) {
            out.writeString(context);
        }
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startObject(CONTEXTS.getPreferredName());
        for (String contextName: this.contextNames) {
            builder.startObject(contextName).endObject();
        }
        builder.endObject().endObject(); // CONTEXTS
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
        return contextNames.equals(that.contextNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextNames);
    }
}
