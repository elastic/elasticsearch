/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class SynonymRule implements Writeable, ToXContentObject {

    public static final ParseField SYNONYMS_FIELD = new ParseField("synonyms");
    public static final ParseField ID_FIELD = new ParseField("id");

    private static final ConstructingObjectParser<SynonymRule, Void> PARSER = new ConstructingObjectParser<>("synonym_rule", args -> {
        @SuppressWarnings("unchecked")
        final String id = (String) args[0];
        final String synonyms = (String) args[1];
        return new SynonymRule(id, synonyms);
    });

    static {
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SYNONYMS_FIELD);
    }

    private final String synonyms;
    private final String id;

    public SynonymRule(@Nullable String id, String synonyms) {
        this.id = Objects.requireNonNullElse(id, UUIDs.base64UUID());
        this.synonyms = synonyms;
    }

    public SynonymRule(StreamInput in) throws IOException {
        this.id = in.readOptionalString();
        this.synonyms = in.readString();
    }

    public static SynonymRule fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (id != null) {
                builder.field(ID_FIELD.getPreferredName(), id);
            }
            builder.field(SYNONYMS_FIELD.getPreferredName(), synonyms);
        }
        builder.endObject();

        return builder;
    }

    public String synonyms() {
        return synonyms;
    }

    public String id() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(id);
        out.writeString(synonyms);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynonymRule that = (SynonymRule) o;
        return Objects.equals(synonyms, that.synonyms) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(synonyms, id);
    }

    /**
     * A simple validation of a synonym rule outside of the context of a specific analyzer.
     *
     * Adopted from org.apache.lucene.analysis.synonym.SolrSynonymParser,
     * with modifications not to invoke analyze method.
     *
     * @return null if the synonym rule is valid, otherwise return the error message.
     */
    public String validate() {
        if (synonyms.length() == 0) {
            return "[synonyms] field can't be empty";
        }
        String[] sides = split(synonyms, "=>");
        if (sides.length > 1) { // explicit mapping
            if (sides.length != 2) {
                return "More than one explicit mapping specified in the same synonyms rule: [" + synonyms + "]";
            }
            String[] inputs = split(sides[0], ",");
            for (String input : inputs) {
                if (input.trim().length() == 0) {
                    return "Incorrect syntax for [synonyms]: [" + synonyms + "]";
                }
            }
            String[] outputs = split(sides[1], ",");
            for (String output : outputs) {
                if (output.trim().length() == 0) {
                    return "Incorrect syntax for [synonyms]: [" + synonyms + "]";
                }
            }
        } else {
            String[] inputs = split(synonyms, ",");
            for (String input : inputs) {
                if (input.trim().length() == 0) {
                    return "Incorrect syntax for [synonyms]: [" + synonyms + "]";
                }
            }
        }
        return null;
    }

    // Copied from org.apache.lucene.analysis.synonym.SolrSynonymParser
    // TODO: should we use String::split instead?
    private static String[] split(String s, String separator) {
        ArrayList<String> list = new ArrayList<>(2);
        StringBuilder sb = new StringBuilder();
        int pos = 0, end = s.length();
        while (pos < end) {
            if (s.startsWith(separator, pos)) {
                if (sb.length() > 0) {
                    list.add(sb.toString());
                    sb = new StringBuilder();
                }
                pos += separator.length();
                continue;
            }

            char ch = s.charAt(pos++);
            if (ch == '\\') {
                sb.append(ch);
                if (pos >= end) break; // ERROR, or let it go?
                ch = s.charAt(pos++);
            }

            sb.append(ch);
        }

        if (sb.length() > 0) {
            list.add(sb.toString());
        }

        return list.toArray(new String[list.size()]);
    }
}
