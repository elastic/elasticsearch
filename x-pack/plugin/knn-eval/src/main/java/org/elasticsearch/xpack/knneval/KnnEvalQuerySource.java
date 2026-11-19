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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Where queries come from, selected by {@code from}: {@code docs} samples stored vectors, {@code vectors} takes the caller's. */
sealed interface KnnEvalQuerySource extends Writeable, ToXContentObject permits KnnEvalQuerySource.DocsSource,
    KnnEvalQuerySource.VectorsSource {

    ParseField QUERY_SOURCE_FIELD = new ParseField("query_source");
    ParseField FROM_FIELD = new ParseField("from");
    ParseField SIZE_FIELD = new ParseField("size");
    ParseField SEED_FIELD = new ParseField("seed");
    ParseField VECTORS_FIELD = new ParseField("vectors");

    /** Serialized by ordinal: append new kinds, never reorder. */
    enum Kind {
        DOCS("docs"),
        VECTORS("vectors");

        private final String from;

        Kind(String from) {
            this.from = from;
        }

        /** The {@code from} value selecting this kind. */
        String from() {
            return from;
        }

        static Kind fromString(String from) {
            for (Kind kind : values()) {
                if (kind.from.equals(from)) {
                    return kind;
                }
            }
            throw new IllegalArgumentException(
                "unknown [from] value [" + from + "]; expected one of " + Arrays.stream(values()).map(Kind::from).toList()
            );
        }
    }

    Kind kind();

    /** The {@code from} value, echoed in responses so results record how queries were chosen. */
    default String from() {
        return kind().from();
    }

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
        return switch (Kind.fromString(from)) {
            case DOCS -> {
                if (size == null) {
                    throw new IllegalArgumentException("[size] is required when [from] is [docs]");
                }
                yield new DocsSource(new KnnEvalSample(size, seed));
            }
            case VECTORS -> {
                if (vectors == null) {
                    throw new IllegalArgumentException("[vectors] is required when [from] is [vectors]");
                }
                yield new VectorsSource(vectors);
            }
        };
    }

    static KnnEvalQuerySource read(StreamInput in) throws IOException {
        return switch (in.readEnum(Kind.class)) {
            case DOCS -> new DocsSource(new KnnEvalSample(in));
            case VECTORS -> new VectorsSource(in.readCollectionAsList(KnnEvalQuery::new));
        };
    }

    /** Samples vectors from stored documents server-side. */
    record DocsSource(KnnEvalSample sample) implements KnnEvalQuerySource {
        public DocsSource {
            Objects.requireNonNull(sample, "sample must not be null");
        }

        @Override
        public Kind kind() {
            return Kind.DOCS;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(kind());
            sample.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FROM_FIELD.getPreferredName(), from());
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
        public Kind kind() {
            return Kind.VECTORS;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(kind());
            out.writeCollection(vectors);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FROM_FIELD.getPreferredName(), from());
            builder.startArray(VECTORS_FIELD.getPreferredName());
            for (KnnEvalQuery query : vectors) {
                query.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }
}
