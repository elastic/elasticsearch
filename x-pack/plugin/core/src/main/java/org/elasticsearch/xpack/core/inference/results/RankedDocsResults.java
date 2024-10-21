/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class RankedDocsResults implements InferenceServiceResults {
    public static final String NAME = "rerank_service_results";
    public static final String RERANK = TaskType.RERANK.toString();

    List<RankedDoc> rankedDocs;

    public RankedDocsResults(List<RankedDoc> rankedDocs) {
        this.rankedDocs = rankedDocs;
    }

    public RankedDocsResults(StreamInput in) throws IOException {
        this.rankedDocs = in.readCollectionAsList(RankedDoc::of);
    }

    public static final ParseField RERANK_FIELD = new ParseField(RERANK);

    public static ConstructingObjectParser<RankedDocsResults, Void> createParser(boolean ignoreUnknownFields) {
        @SuppressWarnings("unchecked")
        ConstructingObjectParser<RankedDocsResults, Void> parser = new ConstructingObjectParser<>(
            "ranked_doc_results",
            ignoreUnknownFields,
            a -> new RankedDocsResults((List<RankedDoc>) a[0])

        );
        parser.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> RankedDoc.createParser(true).apply(p, c),
            RERANK_FIELD
        );
        return parser;
    }

    /**
     * A record representing a document that has been ranked by the cohere rerank API
     * @param index the index of the document when it was passed to the cohere rerank API
     * @param relevanceScore
     * @param text
     */
    public record RankedDoc(int index, float relevanceScore, @Nullable String text)
        implements
            Comparable<RankedDoc>,
            Writeable,
            ToXContentObject {

        public static ConstructingObjectParser<RankedDoc, Void> createParser(boolean ignoreUnknownFields) {
            ConstructingObjectParser<RankedDoc, Void> parser = new ConstructingObjectParser<>(
                "ranked_doc",
                ignoreUnknownFields,
                a -> new RankedDoc((int) a[0], (float) a[1], (String) a[2])

            );
            parser.declareInt(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
            parser.declareFloat(ConstructingObjectParser.constructorArg(), RELEVANCE_SCORE_FIELD);
            parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TEXT_FIELD);
            return parser;
        }

        public static final String NAME = "ranked_doc";
        public static final String INDEX = "index";
        public static final String RELEVANCE_SCORE = "relevance_score";
        public static final String TEXT = "text";

        public static final ParseField INDEX_FIELD = new ParseField(INDEX);
        public static final ParseField RELEVANCE_SCORE_FIELD = new ParseField(RELEVANCE_SCORE);
        public static final ParseField TEXT_FIELD = new ParseField(TEXT);

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(INDEX, index);
            builder.field(RELEVANCE_SCORE, relevanceScore);
            if (text != null) {
                builder.field(TEXT, text);
            }

            builder.endObject();

            return builder;
        }

        public static RankedDoc of(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                return new RankedDoc(in.readInt(), in.readFloat(), in.readOptionalString());
            } else if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                return new RankedDoc(in.readInt(), in.readFloat(), in.readString());
            } else {
                return new RankedDoc(Integer.parseInt(in.readString()), Float.parseFloat(in.readString()), in.readString());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                out.writeInt(index);
                out.writeFloat(relevanceScore);
                out.writeOptionalString(text);
            } else if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeInt(index);
                out.writeFloat(relevanceScore);
                out.writeString(text == null ? "" : text);
            } else {
                out.writeString(Integer.toString(index));
                out.writeString(Float.toString(relevanceScore));
                out.writeString(text == null ? "" : text);
            }
        }

        public Map<String, Object> asMap() {
            return Map.of(NAME, Map.of(INDEX, index, RELEVANCE_SCORE, relevanceScore, TEXT, text));
        }

        @Override
        public int compareTo(RankedDoc other) {
            return Float.compare(other.relevanceScore, this.relevanceScore);
        }

        public String toString() {
            return "RankedDoc{"
                + "index='"
                + index
                + '\''
                + ", relevanceScore='"
                + relevanceScore
                + '\''
                + ", text='"
                + text
                + '\''
                + ", hashcode="
                + hashCode()
                + '}';
        }
    };

    public RankedDocsResults() {
        this.rankedDocs = new ArrayList<RankedDoc>(0);
    }

    public List<RankedDoc> getRankedDocs() {
        return this.rankedDocs;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params).array(RERANK, rankedDocs.iterator());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(rankedDocs);
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("Coordination format not supported by " + NAME);
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException("Legacy format not supported by " + NAME);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(RERANK, rankedDocs.stream().map(RankedDoc::asMap).collect(Collectors.toList()));
        return map;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RankedDocsResults@");
        sb.append(Integer.toHexString(hashCode()));
        sb.append("\n");
        for (RankedDoc rankedDoc : rankedDocs) {
            sb.append(rankedDoc.toString());
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankedDocsResults that = (RankedDocsResults) o;
        return Objects.equals(rankedDocs, that.rankedDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rankedDocs);
    }

}
