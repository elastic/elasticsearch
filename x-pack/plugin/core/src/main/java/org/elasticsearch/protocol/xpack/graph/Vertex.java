/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.graph;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A vertex in a graph response represents a single term (a field and value pair)
 * which appears in one or more documents found as part of the graph exploration.
 *
 * A vertex term could be a bank account number, an email address, a hashtag or any
 * other term that appears in documents and is interesting to represent in a network.
 */
public class Vertex implements ToXContentFragment {

    private final String field;
    private final String term;
    private double weight;
    private final int depth;
    private final long bg;
    private long fg;
    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField TERM = new ParseField("term");
    private static final ParseField WEIGHT = new ParseField("weight");
    private static final ParseField DEPTH = new ParseField("depth");
    private static final ParseField FG = new ParseField("fg");
    private static final ParseField BG = new ParseField("bg");

    public Vertex(String field, String term, double weight, int depth, long bg, long fg) {
        super();
        this.field = field;
        this.term = term;
        this.weight = weight;
        this.depth = depth;
        this.bg = bg;
        this.fg = fg;
    }

    static Vertex readFrom(StreamInput in) throws IOException {
        return new Vertex(in.readString(), in.readString(), in.readDouble(), in.readVInt(), in.readVLong(), in.readVLong());
    }

    void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(term);
        out.writeDouble(weight);
        out.writeVInt(depth);
        out.writeVLong(bg);
        out.writeVLong(fg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, term, weight, depth, bg, fg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Vertex other = (Vertex) obj;
        return depth == other.depth
            && weight == other.weight
            && bg == other.bg
            && fg == other.fg
            && Objects.equals(field, other.field)
            && Objects.equals(term, other.term);

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean returnDetailedInfo = params.paramAsBoolean(GraphExploreResponse.RETURN_DETAILED_INFO_PARAM, false);
        builder.field(FIELD.getPreferredName(), field);
        builder.field(TERM.getPreferredName(), term);
        builder.field(WEIGHT.getPreferredName(), weight);
        builder.field(DEPTH.getPreferredName(), depth);
        if (returnDetailedInfo) {
            builder.field(FG.getPreferredName(), fg);
            builder.field(BG.getPreferredName(), bg);
        }
        return builder;
    }

    private static final ConstructingObjectParser<Vertex, Void> PARSER = new ConstructingObjectParser<>("VertexParser", true, args -> {
        String field = (String) args[0];
        String term = (String) args[1];
        double weight = (Double) args[2];
        int depth = (Integer) args[3];
        Long optionalBg = (Long) args[4];
        Long optionalFg = (Long) args[5];
        long bg = optionalBg == null ? 0 : optionalBg;
        long fg = optionalFg == null ? 0 : optionalFg;
        return new Vertex(field, term, weight, depth, bg, fg);
    });

    static {
        PARSER.declareString(constructorArg(), FIELD);
        PARSER.declareString(constructorArg(), TERM);
        PARSER.declareDouble(constructorArg(), WEIGHT);
        PARSER.declareInt(constructorArg(), DEPTH);
        PARSER.declareLong(optionalConstructorArg(), BG);
        PARSER.declareLong(optionalConstructorArg(), FG);
    }

    static Vertex fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    /**
     * @return a {@link VertexId} object that uniquely identifies this Vertex
     */
    public VertexId getId() {
        return createId(field, term);
    }

    /**
     * A convenience method for creating a {@link VertexId}
     * @param field the field
     * @param term the term
     * @return a {@link VertexId} that can be used for looking up vertices
     */
    public static VertexId createId(String field, String term) {
        return new VertexId(field, term);
    }

    @Override
    public String toString() {
        return getId().toString();
    }

    public String getField() {
        return field;
    }

    public String getTerm() {
        return term;
    }

    /**
     * The weight of a vertex is an accumulation of all of the {@link Connection}s
     * that are linked to this {@link Vertex} as part of a graph exploration.
     * It is used internally to identify the most interesting vertices to be returned.
     * @return a measure of the {@link Vertex}'s relative importance.
     */
    public double getWeight() {
        return weight;
    }

    public void setWeight(final double weight) {
        this.weight = weight;
    }

    // tag::noformat
    /**
     * If the {@link GraphExploreRequest#useSignificance(boolean)} is true (the default)
     * this statistic is available.
     * @return the number of documents in the index that contain this term (see bg_count in
     * <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significantterms-aggregation.html"
     * >the significant_terms aggregation</a>)
     */
    // end::noformat
    public long getBg() {
        return bg;
    }

    // tag::noformat
    /**
     * If the {@link GraphExploreRequest#useSignificance(boolean)} is true (the default)
     * this statistic is available.
     * Together with {@link #getBg()} these numbers are used to derive the significance of a term.
     * @return the number of documents in the sample of best matching documents that contain this term (see fg_count in
     * <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significantterms-aggregation.html"
     * >the significant_terms aggregation</a>)
     */
    // end::noformat
    public long getFg() {
        return fg;
    }

    public void setFg(final long fg) {
        this.fg = fg;
    }

    /**
     * @return the sequence number in the series of hops where this Vertex term was first encountered
     */
    public int getHopDepth() {
        return depth;
    }

    /**
     * An identifier (implements hashcode and equals) that represents a
     * unique key for a {@link Vertex}
     */
    public static class VertexId {
        private final String field;
        private final String term;

        public VertexId(String field, String term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VertexId vertexId = (VertexId) o;
            return Objects.equals(field, vertexId.field) && Objects.equals(term, vertexId.term);
        }

        @Override
        public int hashCode() {
            int result = field != null ? field.hashCode() : 0;
            result = 31 * result + (term != null ? term.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return field + ":" + term;
        }
    }

}
