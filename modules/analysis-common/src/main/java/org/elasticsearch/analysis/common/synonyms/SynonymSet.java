/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common.synonyms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SynonymSet implements Writeable, ToXContentObject {

    public static final ParseField SYNONYMS_FIELD = new ParseField("synonyms");
    private static final ConstructingObjectParser<SynonymSet, Void> PARSER = new ConstructingObjectParser<>("synonyms", args -> {
        @SuppressWarnings("unchecked")
        final Map<String, String> synonyms = (Map<String, String>) args[0];
        return new SynonymSet(synonyms);
    });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), SYNONYMS_FIELD);
    }

    private final Map<String, String> synonyms;

    public SynonymSet(Map<String, String> synonyms) {
        Objects.requireNonNull(synonyms, "synonyms cannot be null");
        this.synonyms = synonyms;

        synonyms.forEach((key, value) -> {
            if (Strings.isEmpty(value)) {
                throw new IllegalArgumentException("synonym with key [" + key + "] has an empty value");
            }
        });
    }

    public SynonymSet(StreamInput in) throws IOException {
        this.synonyms = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    public static SynonymSet fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SYNONYMS_FIELD.getPreferredName(), synonyms);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(synonyms, StreamOutput::writeString, StreamOutput::writeString);
    }
}
