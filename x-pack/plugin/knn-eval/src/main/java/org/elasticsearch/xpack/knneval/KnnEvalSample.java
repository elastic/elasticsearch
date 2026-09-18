/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.Strings;
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

/** Selects indexed query vectors. An optional seed makes the sample reproducible. */
record KnnEvalSample(int size, @Nullable Integer seed) implements Writeable, ToXContentObject {

    /** Each sampled query costs one baseline plus one search per knob set, so this bounds fan-out. */
    static final int MAX_SAMPLE_SIZE = 10_000;

    static final ParseField SIZE_FIELD = new ParseField("size");
    static final ParseField SEED_FIELD = new ParseField("seed");

    private static final ConstructingObjectParser<KnnEvalSample, Void> PARSER = new ConstructingObjectParser<>(
        "knn_eval_sample",
        args -> new KnnEvalSample((Integer) args[0], (Integer) args[1])
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SIZE_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), SEED_FIELD);
    }

    /** Creates a bounded seeded or unseeded sample. */
    KnnEvalSample {
        if (size < 1 || size > MAX_SAMPLE_SIZE) {
            throw new IllegalArgumentException("[" + SIZE_FIELD.getPreferredName() + "] must be between 1 and " + MAX_SAMPLE_SIZE);
        }
    }

    KnnEvalSample(StreamInput in) throws IOException {
        this(in.readVInt(), in.readOptionalInt());
    }

    static KnnEvalSample fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public int getSize() {
        return size;
    }

    @Nullable
    public Integer getSeed() {
        return seed;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        out.writeOptionalInt(seed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SIZE_FIELD.getPreferredName(), size);
        if (seed != null) {
            builder.field(SEED_FIELD.getPreferredName(), seed);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
