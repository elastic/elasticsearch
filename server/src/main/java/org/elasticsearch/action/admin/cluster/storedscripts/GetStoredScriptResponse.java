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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetStoredScriptResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField _ID_PARSE_FIELD = new ParseField("_id");
    public static final ParseField FOUND_PARSE_FIELD = new ParseField("found");
    public static final ParseField SCRIPT = new ParseField("script");

    private static final ConstructingObjectParser<GetStoredScriptResponse, String> PARSER =
        new ConstructingObjectParser<>("GetStoredScriptResponse",
            true,
            (a, c) -> {
                String id = (String) a[0];
                boolean found = (Boolean)a[1];
                StoredScriptSource scriptSource = (StoredScriptSource)a[2];
                return found ? new GetStoredScriptResponse(id, scriptSource) : new GetStoredScriptResponse(id, null);
            });

    static {
        PARSER.declareField(constructorArg(), (p, c) -> p.text(),
            _ID_PARSE_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), (p, c) -> p.booleanValue(),
            FOUND_PARSE_FIELD, ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> StoredScriptSource.fromXContent(p, true),
            SCRIPT, ObjectParser.ValueType.OBJECT);
    }

    private Map<String, StoredScriptSource> storedScripts;

    GetStoredScriptResponse(StreamInput in) throws IOException {
        super(in);

        int size = in.readVInt();
        storedScripts = new HashMap<>(size);
        for (int i = 0 ; i < size ; i++) {
            String id = in.readString();
            storedScripts.put(id, new StoredScriptSource(in));
        }
    }

    GetStoredScriptResponse(String id, StoredScriptSource source) {
        this.storedScripts = new HashMap<>();
        storedScripts.put(id, source);
    }

    GetStoredScriptResponse(Map<String, StoredScriptSource> storedScripts) {
        this.storedScripts = storedScripts;
    }

    public Map<String, StoredScriptSource> getStoredScripts() {
        return storedScripts;
    }

    /**
     * @deprecated - Needed for backwards compatibility.
     * Use {@link #getStoredScripts()} instead
     *
     * @return if a stored script and if not found <code>null</code>
     */
    @Deprecated
    public StoredScriptSource getSource() {
        if (storedScripts != null && storedScripts.size() == 1) {
            return storedScripts.entrySet().iterator().next().getValue();
        } else {
            return null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
//        ScriptMetaData scriptMetaData = new ScriptMetaData(getStoredScripts());
//        return scriptMetaData.toXContent(builder, params);
        builder.startObject();

        Map<String, StoredScriptSource> storedScripts = getStoredScripts();
        if (storedScripts != null) {
            for (Map.Entry<String, StoredScriptSource> storedScript : storedScripts.entrySet()) {
                builder.startObject(storedScript.getKey());
                builder.field(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName());
                storedScript.getValue().toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    public static GetStoredScriptResponse fromXContent(XContentParser parser) throws IOException {
        final Map<String, StoredScriptSource> storedScripts = new HashMap<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                parser.nextToken();
                parser.nextToken();
                StoredScriptSource storedScriptSource = StoredScriptSource.fromXContent(parser, false);
                storedScripts.put(name, storedScriptSource);
            }
        }
        return new GetStoredScriptResponse(storedScripts);
    }

//    public static GetStoredScriptResponse fromXContent(XContentParser parser) throws IOException {
//        return PARSER.parse(parser, null);
//    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (storedScripts == null ) {
            out.writeVInt(0);
            return;
        }

        out.writeVInt(storedScripts.size());
        for (Map.Entry<String, StoredScriptSource> storedScript : storedScripts.entrySet()) {
            out.writeString(storedScript.getKey());
            storedScript.getValue().writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetStoredScriptResponse that = (GetStoredScriptResponse) o;
        return Objects.equals(storedScripts, that.storedScripts);
    }

    @Override
    public int hashCode() {
        return storedScripts.hashCode();
    }
}
