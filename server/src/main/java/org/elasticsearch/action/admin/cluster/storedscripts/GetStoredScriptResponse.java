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

    private static final ParseField _ID_PARSE_FIELD = new ParseField("_id");
    private static final ParseField FOUND_PARSE_FIELD = new ParseField("found");
    private static final ParseField SCRIPT = new ParseField("script");

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

    private final Map<String, StoredScriptSource> storedScripts;
    private final String[] requestedIds;

    GetStoredScriptResponse(StreamInput in) throws IOException {
        super(in);

        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            storedScripts = in.readMap(StreamInput::readString, StoredScriptSource::new);
        } else {
            StoredScriptSource source;
            if (in.readBoolean()) {
                source = new StoredScriptSource(in);
            } else {
                source = null;
            }
            String id = in.readString();
            storedScripts = new HashMap<>(1);
            storedScripts.put(id, source);
        }
        requestedIds = new String[0];
    }

    GetStoredScriptResponse(String id, StoredScriptSource source) {
        this.storedScripts = new HashMap<>();
        storedScripts.put(id, source);
        requestedIds = new String[]{ id };
    }

    GetStoredScriptResponse(String[] requestedIds, Map<String, StoredScriptSource> storedScripts) {
        this.requestedIds = requestedIds;
        this.storedScripts = storedScripts;
    }

    public Map<String, StoredScriptSource> getStoredScripts() {
        return storedScripts;
    }

    /**
     * Helper method to return a single stored script
     *
     * @param id the id of the script
     * @return the script source or null if not found
     */
    public StoredScriptSource getStoredScript(String id) {
        return storedScripts.get(id);
    }

    /**
     * @deprecated - Needed for backwards compatibility.
     * Use {@link #getStoredScript(String)} ()} instead
     *
     * @return if a stored script and if not found <code>null</code>
     */
    @Deprecated
    public StoredScriptSource getSource() {
        return storedScripts.entrySet().iterator().next().getValue();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean isSingleId = requestedIds.length == 1 && storedScripts.size() == 1;
        if (!params.paramAsBoolean("new_format", false) && isSingleId) {
            return toXContentPre80(builder, params);
        }

        builder.startObject();
        Map<String, StoredScriptSource> storedScripts = getStoredScripts();
        if (storedScripts != null) {
            for (Map.Entry<String, StoredScriptSource> storedScript : storedScripts.entrySet()) {
                builder.field(storedScript.getKey());
                storedScript.getValue().toXContent(builder, params);
            }
        }
        builder.endObject();
        return builder;
    }

    /**
     * The original format is the default prior to 8.0 and needed for backwards compatibility
     * @see #fromXContentNewFormat(XContentParser)
     */
    @Deprecated
    public static GetStoredScriptResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static GetStoredScriptResponse fromXContentNewFormat(XContentParser parser) throws IOException {
        final Map<String, StoredScriptSource> storedScripts = new HashMap<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                assert parser.nextToken() == XContentParser.Token.START_OBJECT;
                StoredScriptSource storedScriptSource = StoredScriptSource.fromXContent(parser, false);
                storedScripts.put(name, storedScriptSource);
            }
        }
        return new GetStoredScriptResponse(storedScripts.keySet().toArray(String[]::new), storedScripts);
    }

    @Deprecated
    private XContentBuilder toXContentPre80(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        String id = requestedIds[0];
        StoredScriptSource source = null;
        if (!storedScripts.isEmpty()) {
            Map.Entry<String, StoredScriptSource> entry = storedScripts.entrySet().iterator().next();
            source = entry.getValue();
        }

        builder.field(_ID_PARSE_FIELD.getPreferredName(), id);
        builder.field(FOUND_PARSE_FIELD.getPreferredName(), source != null);
        if (source != null) {
            builder.field(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName());
            source.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            if (storedScripts == null ) {
                out.writeVInt(0);
                return;
            }

            out.writeVInt(storedScripts.size());
            for (Map.Entry<String, StoredScriptSource> storedScript : storedScripts.entrySet()) {
                out.writeString(storedScript.getKey());
                storedScript.getValue().writeTo(out);
            }
        } else {
            Map.Entry<String, StoredScriptSource> entry = storedScripts.entrySet().iterator().next();
            if (entry.getValue() == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                entry.getValue().writeTo(out);
            }
            out.writeString(entry.getKey());
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
