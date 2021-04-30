/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.tokenisation;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class Vocabulary implements ToXContentObject {

    public static final String NAME = "vocab";
    public static final ParseField VOCAB = new ParseField(NAME);
    public static final ParseField UNKNOWN_TOKEN = new ParseField("unknown");

    private static final ConstructingObjectParser<Vocabulary, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<Vocabulary, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<Vocabulary, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Vocabulary, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new Vocabulary((List<String>) a[0], (Integer) a[1]));

        parser.declareStringArray(ConstructingObjectParser.constructorArg(), VOCAB);
        parser.declareInt(ConstructingObjectParser.constructorArg(), UNKNOWN_TOKEN);

        return parser;
    }

    public static Vocabulary fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final SortedMap<String, Integer> vocab;
    private final int unknownToken;

    public Vocabulary(List<String> words, int unknownToken) {
        this.unknownToken = unknownToken;
        vocab = new TreeMap<>();
        for (int i = 0; i < words.size(); i++) {
            vocab.put(words.get(i), i);
        }
    }

    public int token(String word) {
        Integer token = vocab.get(word);
        if (token == null) {
            token = unknownToken;
        }
        return token;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(VOCAB.getPreferredName(), vocab.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vocabulary that = (Vocabulary) o;
        return unknownToken == that.unknownToken && Objects.equals(vocab, that.vocab);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocab, unknownToken);
    }
}
