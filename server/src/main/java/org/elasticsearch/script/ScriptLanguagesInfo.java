/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The allowable types, languages and their corresponding contexts.  When serialized there is a top level <code>types_allowed</code> list,
 * meant to reflect the setting <code>script.allowed_types</code> with the allowed types (eg <code>inline</code>, <code>stored</code>).
 *
 * The top-level <code>language_contexts</code> list of objects have the <code>language</code> (eg. <code>painless</code>,
 * <code>mustache</code>) and a list of <code>contexts</code> available for the language.  It is the responsibility of the caller to ensure
 * these contexts are filtered by the <code>script.allowed_contexts</code> setting.
 *
 * The json serialization of the object has the form:
 * <code>
 *     {
 *   "types_allowed": [
 *     "inline",
 *     "stored"
 *   ],
 *   "language_contexts": [
 *     {
 *       "language": "expression",
 *       "contexts": [
 *         "aggregation_selector",
 *         "aggs"
 *         ...
 *       ]
 *     },
 *     {
 *       "language": "painless",
 *       "contexts": [
 *         "aggregation_selector",
 *         "aggs",
 *         "aggs_combine",
 *         ...
 *       ]
 *     }
 * ...
 *   ]
 * }
 * </code>
 */
public class ScriptLanguagesInfo implements ToXContentObject, Writeable {
    public static final ParseField TYPES_ALLOWED = new ParseField("types_allowed");
    public static final ParseField LANGUAGE_CONTEXTS = new ParseField("language_contexts");
    public static final ParseField LANGUAGE = new ParseField("language");
    public static final ParseField CONTEXTS = new ParseField("contexts");

    public final Set<String> typesAllowed;
    public final Map<String, Set<String>> languageContexts;

    public ScriptLanguagesInfo(Set<String> typesAllowed, Map<String, Set<String>> languageContexts) {
        this.typesAllowed = typesAllowed != null ? Set.copyOf(typesAllowed) : Collections.emptySet();
        this.languageContexts = languageContexts != null ? Map.copyOf(languageContexts) : Collections.emptyMap();
    }

    public ScriptLanguagesInfo(StreamInput in) throws IOException {
        typesAllowed = in.readCollectionAsImmutableSet(StreamInput::readString);
        languageContexts = in.readImmutableMap(sin -> sin.readCollectionAsImmutableSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(typesAllowed);
        out.writeMap(languageContexts, StreamOutput::writeStringCollection);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptLanguagesInfo that = (ScriptLanguagesInfo) o;
        return Objects.equals(typesAllowed, that.typesAllowed) && Objects.equals(languageContexts, that.languageContexts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typesAllowed, languageContexts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startArray(TYPES_ALLOWED.getPreferredName());
        for (String type : typesAllowed.stream().sorted().toList()) {
            builder.value(type);
        }

        builder.endArray().startArray(LANGUAGE_CONTEXTS.getPreferredName());
        List<Map.Entry<String, Set<String>>> languagesByName = languageContexts.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .toList();

        for (Map.Entry<String, Set<String>> languageContext : languagesByName) {
            builder.startObject().field(LANGUAGE.getPreferredName(), languageContext.getKey()).startArray(CONTEXTS.getPreferredName());
            for (String context : languageContext.getValue().stream().sorted().toList()) {
                builder.value(context);
            }
            builder.endArray().endObject();
        }

        return builder.endArray().endObject();
    }
}
