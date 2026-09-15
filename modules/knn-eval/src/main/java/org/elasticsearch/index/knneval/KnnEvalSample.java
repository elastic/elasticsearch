/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

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
import java.util.Objects;

/**
 * Draws the query set from the indexed documents, which keeps the query distribution matched to the indexed one without shipping
 * high-dimensional vectors up just to send them straight back down. A {@link #getSeed() seed} makes the draw reproducible, so the
 * sweeps making up one recall curve share a query set.
 */
public class KnnEvalSample implements Writeable, ToXContentObject {

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

    private final int size;
    @Nullable
    private final Integer seed;

    public KnnEvalSample(int size, @Nullable Integer seed) {
        if (size < 1 || size > MAX_SAMPLE_SIZE) {
            throw new IllegalArgumentException("[" + SIZE_FIELD.getPreferredName() + "] must be between 1 and " + MAX_SAMPLE_SIZE);
        }
        this.size = size;
        this.seed = seed;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KnnEvalSample other = (KnnEvalSample) obj;
        return size == other.size && Objects.equals(seed, other.seed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, seed);
    }
}
