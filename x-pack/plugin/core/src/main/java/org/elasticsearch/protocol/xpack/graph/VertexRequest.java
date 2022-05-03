/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.graph;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest.TermBoost;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A request to identify terms from a choice of field as part of a {@link Hop}.
 * Optionally, a set of terms can be provided that are used as an exclusion or
 * inclusion list to filter which terms are considered.
 *
 */
public class VertexRequest implements ToXContentObject {
    public static final int DEFAULT_SIZE = 5;
    public static final int DEFAULT_MIN_DOC_COUNT = 3;
    public static final int DEFAULT_SHARD_MIN_DOC_COUNT = 2;

    private String fieldName;
    private int size = DEFAULT_SIZE;
    private Map<String, TermBoost> includes;
    private Set<String> excludes;
    private int minDocCount = DEFAULT_MIN_DOC_COUNT;
    private int shardMinDocCount = DEFAULT_SHARD_MIN_DOC_COUNT;

    public VertexRequest() {

    }

    void readFrom(StreamInput in) throws IOException {
        fieldName = in.readString();
        size = in.readVInt();
        minDocCount = in.readVInt();
        shardMinDocCount = in.readVInt();

        int numIncludes = in.readVInt();
        if (numIncludes > 0) {
            includes = new HashMap<>();
            for (int i = 0; i < numIncludes; i++) {
                TermBoost tb = new TermBoost();
                tb.readFrom(in);
                includes.put(tb.term, tb);
            }
        }

        int numExcludes = in.readVInt();
        if (numExcludes > 0) {
            excludes = new HashSet<>();
            for (int i = 0; i < numExcludes; i++) {
                excludes.add(in.readString());
            }
        }

    }

    void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeVInt(size);
        out.writeVInt(minDocCount);
        out.writeVInt(shardMinDocCount);

        if (includes != null) {
            out.writeVInt(includes.size());
            for (TermBoost tb : includes.values()) {
                tb.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }

        if (excludes != null) {
            out.writeVInt(excludes.size());
            for (String term : excludes) {
                out.writeString(term);
            }
        } else {
            out.writeVInt(0);
        }
    }

    public String fieldName() {
        return fieldName;
    }

    public VertexRequest fieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public int size() {
        return size;
    }

    /**
     * @param size The maximum number of terms that should be returned from this field as part of this {@link Hop}
     */
    public VertexRequest size(int size) {
        this.size = size;
        return this;
    }

    public boolean hasIncludeClauses() {
        return includes != null && includes.size() > 0;
    }

    public boolean hasExcludeClauses() {
        return excludes != null && excludes.size() > 0;
    }

    /**
     * Adds a term that should be excluded from results
     * @param term A term to be excluded
     */
    public void addExclude(String term) {
        if (includes != null) {
            throw new IllegalArgumentException("Cannot have both include and exclude clauses");
        }
        if (excludes == null) {
            excludes = new HashSet<>();
        }
        excludes.add(term);
    }

    /**
     * Adds a term to the set of allowed values - the boost defines the relative
     * importance when pursuing connections in subsequent {@link Hop}s. The boost value
     * appears as part of the query.
     * @param term a required term
     * @param boost an optional boost
     */
    public void addInclude(String term, float boost) {
        if (excludes != null) {
            throw new IllegalArgumentException("Cannot have both include and exclude clauses");
        }
        if (includes == null) {
            includes = new HashMap<>();
        }
        includes.put(term, new TermBoost(term, boost));
    }

    public TermBoost[] includeValues() {
        return includes.values().toArray(new TermBoost[includes.size()]);
    }

    public SortedSet<BytesRef> includeValuesAsSortedSet() {
        SortedSet<BytesRef> set = new TreeSet<>();
        for (String include : includes.keySet()) {
            set.add(new BytesRef(include));
        }
        return set;
    }

    public SortedSet<BytesRef> excludesAsSortedSet() {
        SortedSet<BytesRef> set = new TreeSet<>();
        for (String include : excludes) {
            set.add(new BytesRef(include));
        }
        return set;
    }

    public int minDocCount() {
        return minDocCount;
    }

    /**
     * A "certainty" threshold which defines the weight-of-evidence required before
     * a term found in this field is identified as a useful connection
     *
     * @param value The minimum number of documents that contain this term found in the samples used across all shards
     */
    public VertexRequest minDocCount(int value) {
        minDocCount = value;
        return this;
    }

    public int shardMinDocCount() {
        return Math.min(shardMinDocCount, minDocCount);
    }

    /**
     * A "certainty" threshold which defines the weight-of-evidence required before
     * a term found in this field is identified as a useful connection
     *
     * @param value The minimum number of documents that contain this term found in the samples used across all shards
     */
    public VertexRequest shardMinDocCount(int value) {
        shardMinDocCount = value;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("field", fieldName);
        if (size != DEFAULT_SIZE) {
            builder.field("size", size);
        }
        if (minDocCount != DEFAULT_MIN_DOC_COUNT) {
            builder.field("min_doc_count", minDocCount);
        }
        if (shardMinDocCount != DEFAULT_SHARD_MIN_DOC_COUNT) {
            builder.field("shard_min_doc_count", shardMinDocCount);
        }
        if (includes != null) {
            builder.startArray("include");
            for (TermBoost tb : includes.values()) {
                builder.startObject();
                builder.field("term", tb.term);
                builder.field("boost", tb.boost);
                builder.endObject();
            }
            builder.endArray();
        }
        if (excludes != null) {
            builder.startArray("exclude");
            for (String value : excludes) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

}
