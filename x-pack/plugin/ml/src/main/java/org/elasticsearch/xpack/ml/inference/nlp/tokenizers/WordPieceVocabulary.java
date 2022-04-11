/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class WordPieceVocabulary implements ToXContentObject {

    public static final String NAME = "vocab";
    public static final ParseField VOCAB = new ParseField(NAME);
    public static final ParseField UNKNOWN_TOKEN = new ParseField("unknown");

    private static final ConstructingObjectParser<WordPieceVocabulary, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<WordPieceVocabulary, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<WordPieceVocabulary, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<WordPieceVocabulary, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new WordPieceVocabulary((List<String>) a[0], (Integer) a[1])
        );

        parser.declareStringArray(ConstructingObjectParser.constructorArg(), VOCAB);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), UNKNOWN_TOKEN);

        return parser;
    }

    public static WordPieceVocabulary fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final SortedMap<String, Integer> vocab;
    private final int unknownToken;

    public WordPieceVocabulary(List<String> words, Integer unknownToken) {
        this.unknownToken = unknownToken == null ? -1 : unknownToken;
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
        WordPieceVocabulary that = (WordPieceVocabulary) o;
        return unknownToken == that.unknownToken && Objects.equals(vocab, that.vocab);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocab, unknownToken);
    }
}
