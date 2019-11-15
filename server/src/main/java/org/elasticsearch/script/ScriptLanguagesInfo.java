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

package org.elasticsearch.script;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class ScriptLanguagesInfo implements ToXContentObject, Writeable {
    private static final ParseField TYPES_ALLOWED = new ParseField("types_allowed");
    private static final ParseField LANGUAGE_CONTEXTS = new ParseField("language_contexts");
    private static final ParseField LANGUAGE = new ParseField("language");
    private static final ParseField CONTEXTS = new ParseField("contexts");

    public final Set<String> typesAllowed;
    public final Map<String,Set<String>> languageContexts;

    public ScriptLanguagesInfo(Set<String> typesAllowed, Map<String,Set<String>> languageContexts) {
        this.typesAllowed = typesAllowed != null ? Set.copyOf(typesAllowed): Collections.emptySet();
        this.languageContexts = languageContexts != null ? Map.copyOf(languageContexts): Collections.emptyMap();
    }

    public ScriptLanguagesInfo(StreamInput in) throws IOException {
        typesAllowed = readStringSet(in);
        languageContexts = readStringMapSet(in);
    }

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ScriptLanguagesInfo,Void> PARSER =
        new ConstructingObjectParser<>("script_languages_info", true,
            (a) -> new ScriptLanguagesInfo(
                new HashSet<>((List<String>)a[0]),
                ((List<Tuple<String,Set<String>>>)a[1]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2))
            )
        );

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Tuple<String,Set<String>>,Void> LANGUAGE_CONTEXT_PARSER =
        new ConstructingObjectParser<>("language_contexts", true,
            (m, name) -> new Tuple<>((String)m[0], Set.copyOf((List<String>)m[1]))
        );

    static {
        PARSER.declareStringArray(constructorArg(), TYPES_ALLOWED);
        PARSER.declareObjectArray(constructorArg(), LANGUAGE_CONTEXT_PARSER, LANGUAGE_CONTEXTS);
        LANGUAGE_CONTEXT_PARSER.declareString(constructorArg(), LANGUAGE);
        LANGUAGE_CONTEXT_PARSER.declareStringArray(constructorArg(), CONTEXTS);
    }

    public static ScriptLanguagesInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeStringSet(out, typesAllowed);
        writeStringMapSet(out, languageContexts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ScriptLanguagesInfo that = (ScriptLanguagesInfo) o;
        return Objects.equals(typesAllowed, that.typesAllowed) &&
            Objects.equals(languageContexts, that.languageContexts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typesAllowed, languageContexts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startArray(TYPES_ALLOWED.getPreferredName());
        for (String type: typesAllowed.stream().sorted().collect(Collectors.toList())) {
            builder.value(type);
        }

        builder.endArray().startArray(LANGUAGE_CONTEXTS.getPreferredName());
        List<Map.Entry<String,Set<String>>> languagesByName = languageContexts.entrySet().stream().sorted(
            Map.Entry.comparingByKey()
        ).collect(Collectors.toList());

        for (Map.Entry<String,Set<String>> languageContext: languagesByName) {
            builder.startObject().field(LANGUAGE.getPreferredName(), languageContext.getKey()).startArray(CONTEXTS.getPreferredName());
            for (String context: languageContext.getValue().stream().sorted().collect(Collectors.toList())) {
                builder.value(context);
            }
            builder.endArray().endObject();
        }

        return builder.endArray().endObject();
    }

    private static Map<String,Set<String>> readStringMapSet(StreamInput in) throws IOException {
        Map<String,Set<String>> values = new HashMap<>();
        for (int i = in.readInt(); i > 0; i--) {
            values.put(in.readString(), readStringSet(in));
        }
        return values;
    }

    private static void writeStringMapSet(StreamOutput out, Map<String,Set<String>> values) throws IOException {
        out.writeInt(values.size());
        for (Map.Entry<String,Set<String>> value: values.entrySet()) {
            out.writeString(value.getKey());
            writeStringSet(out, value.getValue());
        }
    }

    private static Set<String> readStringSet(StreamInput in) throws IOException {
        Set<String> values = new HashSet<>();
        for (int i = in.readInt(); i > 0; i--) {
            values.add(in.readString());
        }
        return values;
    }

    private static void writeStringSet(StreamOutput out, Set<String> values) throws IOException {
        out.writeInt(values.size());
        for (String value: values) {
            out.writeString(value);
        }
    }
}
