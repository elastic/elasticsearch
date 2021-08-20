/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Vocabulary implements Writeable {

    private static final ParseField VOCAB = new ParseField("vocab");

    @SuppressWarnings({ "unchecked"})
    public static ConstructingObjectParser<Vocabulary, Void> createParser(boolean ignoreUnkownFields) {
        ConstructingObjectParser<Vocabulary, Void> parser = new ConstructingObjectParser<>("vocabulary", ignoreUnkownFields,
            a -> new Vocabulary((List<String>) a[0]));
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), VOCAB);
        return parser;
    }

    private final List<String> vocab;

    public Vocabulary(List<String> vocab) {
        this.vocab = ExceptionsHelper.requireNonNull(vocab, VOCAB);
    }

    public Vocabulary(StreamInput in) throws IOException {
        vocab = in.readStringList();
    }

    public List<String> get() {
        return vocab;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(vocab);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vocabulary that = (Vocabulary) o;
        return Objects.equals(vocab, that.vocab);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocab);
    }
}
