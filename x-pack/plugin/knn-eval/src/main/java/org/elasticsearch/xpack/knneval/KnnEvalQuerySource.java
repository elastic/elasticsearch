/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Discriminated union: where evaluation queries come from. The {@code from} field selects the sub-type:
 * <ul>
 *   <li>{@code "docs"} — sample vectors from stored documents server-side ({@link DocsSource}).</li>
 *   <li>{@code "vectors"} — caller supplies explicit query vectors ({@link VectorsSource}).</li>
 *   <li>{@code "queries"} — future: sample from stored queries ({@link QueriesSource}).</li>
 * </ul>
 */
sealed interface KnnEvalQuerySource extends Writeable, ToXContentObject permits KnnEvalQuerySource.DocsSource,
    KnnEvalQuerySource.VectorsSource, KnnEvalQuerySource.QueriesSource {

    ParseField QUERY_SOURCE_FIELD = new ParseField("query_source");
    ParseField FROM_FIELD = new ParseField("from");
    ParseField SIZE_FIELD = new ParseField("size");
    ParseField SEED_FIELD = new ParseField("seed");
    ParseField VECTORS_FIELD = new ParseField("vectors");

    static KnnEvalQuerySource fromXContent(XContentParser parser) throws IOException {
        String from = null;
        Integer size = null;
        Integer seed = null;
        List<KnnEvalQuery> vectors = null;

        XContentParser.Token token;
        String currentFieldName = null;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                from = parser.text();
            } else if (SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                size = parser.intValue();
            } else if (SEED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                seed = parser.intValue();
            } else if (VECTORS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token != XContentParser.Token.START_ARRAY) {
                    throw new IOException("expected array for [" + VECTORS_FIELD.getPreferredName() + "] in [query_source]");
                }
                vectors = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    vectors.add(KnnEvalQuery.fromXContent(parser));
                }
            } else {
                throw new IOException("unknown field [" + currentFieldName + "] in [query_source]");
            }
        }
        if (from == null) {
            throw new IllegalArgumentException("[from] is required in [query_source]");
        }
        return switch (from) {
            case "docs" -> {
                if (size == null) {
                    throw new IllegalArgumentException("[size] is required when [from] is [docs]");
                }
                yield new DocsSource(new KnnEvalSample(size, seed));
            }
            case "queries" -> {
                if (size == null) {
                    throw new IllegalArgumentException("[size] is required when [from] is [queries]");
                }
                yield new QueriesSource(size, seed);
            }
            case "vectors" -> {
                if (vectors == null) {
                    throw new IllegalArgumentException("[vectors] is required when [from] is [vectors]");
                }
                yield new VectorsSource(vectors);
            }
            default -> throw new IllegalArgumentException("unknown [from] value [" + from + "]; expected one of [docs, vectors, queries]");
        };
    }

    static KnnEvalQuerySource read(StreamInput in) throws IOException {
        byte discriminator = in.readByte();
        return switch (discriminator) {
            case 0 -> new DocsSource(new KnnEvalSample(in));
            case 1 -> new VectorsSource(in.readCollectionAsList(KnnEvalQuery::new));
            case 2 -> new QueriesSource(in.readVInt(), in.readOptionalInt());
            default -> throw new IOException("unknown KnnEvalQuerySource discriminator: " + discriminator);
        };
    }

    /** Samples vectors from stored documents server-side. */
    record DocsSource(KnnEvalSample sample) implements KnnEvalQuerySource {
        public DocsSource {
            Objects.requireNonNull(sample, "sample must not be null");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte((byte) 0);
            sample.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FROM_FIELD.getPreferredName(), "docs");
            builder.field(SIZE_FIELD.getPreferredName(), sample.getSize());
            if (sample.getSeed() != null) {
                builder.field(SEED_FIELD.getPreferredName(), sample.getSeed());
            }
            builder.endObject();
            return builder;
        }
    }

    /** Uses caller-supplied query vectors. */
    record VectorsSource(List<KnnEvalQuery> vectors) implements KnnEvalQuerySource {
        public VectorsSource {
            vectors = List.copyOf(vectors);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte((byte) 1);
            out.writeCollection(vectors);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FROM_FIELD.getPreferredName(), "vectors");
            builder.startArray(VECTORS_FIELD.getPreferredName());
            for (KnnEvalQuery query : vectors) {
                query.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    /** Placeholder for future stored-query sampling. Not yet implemented. */
    record QueriesSource(int size, @Nullable Integer seed) implements KnnEvalQuerySource {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte((byte) 2);
            out.writeVInt(size);
            out.writeOptionalInt(seed);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FROM_FIELD.getPreferredName(), "queries");
            builder.field(SIZE_FIELD.getPreferredName(), size);
            if (seed != null) {
                builder.field(SEED_FIELD.getPreferredName(), seed);
            }
            builder.endObject();
            return builder;
        }
    }
}
