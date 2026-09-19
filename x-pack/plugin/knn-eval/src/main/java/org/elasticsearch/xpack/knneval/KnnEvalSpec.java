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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Defines queries, a baseline, and candidate settings for one recall-versus-cost evaluation. Exactly one explicit or sampled query
 * source is required.
 */
final class KnnEvalSpec implements Writeable, ToXContentObject {

    static final int MAX_QUERIES = 10_000;
    static final int MAX_K = 1_000;
    static final int MAX_KNN_SETTINGS = 32;
    static final int MAX_QUERIES_PER_BATCH = 100;

    static final ParseField FIELD_FIELD = new ParseField("field");
    static final ParseField K_FIELD = new ParseField("k");
    static final ParseField QUERIES_FIELD = new ParseField("queries");
    static final ParseField SAMPLE_FIELD = new ParseField("sample");
    static final ParseField BASELINE_FIELD = new ParseField("baseline");
    static final ParseField KNN_SETTINGS_FIELD = new ParseField("knn_settings");
    static final ParseField MAX_QUERIES_PER_BATCH_FIELD = new ParseField("max_queries_per_batch");

    /**
     * A bounded default keeps an omitted baseline from unexpectedly scanning every full-precision vector. Exact remains available
     * explicitly for certification runs.
     */
    private static final KnnEvalKnobs DEFAULT_BASELINE = new KnnEvalKnobs(20.0f, null, 100.0f, false);

    /** The coordinator holds every sub-search response of an msearch until the last lands, so batch size, not concurrency, bounds heap. */
    private static final int DEFAULT_MAX_QUERIES_PER_BATCH = 1;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KnnEvalSpec, Void> PARSER = new ConstructingObjectParser<>(
        "knn_eval",
        args -> new KnnEvalSpec(
            (String) args[0],
            (Integer) args[1],
            (List<KnnEvalQuery>) args[2],
            (KnnEvalSample) args[3],
            args[4] == null ? DEFAULT_BASELINE : (KnnEvalKnobs) args[4],
            (List<KnnEvalKnobs>) args[5],
            args[6] == null ? DEFAULT_MAX_QUERIES_PER_BATCH : (Integer) args[6]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), K_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalQuery.fromXContent(p), QUERIES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalSample.fromXContent(p), SAMPLE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalKnobs.fromXContent(p), BASELINE_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> KnnEvalKnobs.fromXContent(p), KNN_SETTINGS_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_QUERIES_PER_BATCH_FIELD);
    }

    private final String field;
    private final int k;
    @Nullable
    private final List<KnnEvalQuery> queries;
    @Nullable
    private final KnnEvalSample sample;
    private final KnnEvalKnobs baseline;
    private final List<KnnEvalKnobs> knnSettings;
    private final int maxQueriesPerBatch;

    /** Creates and validates an evaluation specification with one query source and at least one candidate setting. */
    KnnEvalSpec(
        String field,
        int k,
        @Nullable List<KnnEvalQuery> queries,
        @Nullable KnnEvalSample sample,
        KnnEvalKnobs baseline,
        List<KnnEvalKnobs> knnSettings,
        int maxQueriesPerBatch
    ) {
        validateBounds(field, k, maxQueriesPerBatch);
        validateQuerySource(queries, sample);
        baseline = normalizeBaseline(baseline);
        validateNumCandidates(baseline, k);
        validateCandidates(knnSettings, k);
        this.field = field;
        this.k = k;
        this.queries = queries == null ? null : List.copyOf(queries);
        this.sample = sample;
        this.baseline = baseline;
        this.knnSettings = List.copyOf(knnSettings);
        this.maxQueriesPerBatch = maxQueriesPerBatch;
    }

    private static void validateBounds(String field, int k, int maxQueriesPerBatch) {
        if (Strings.hasText(field) == false) {
            throw new IllegalArgumentException("[" + FIELD_FIELD.getPreferredName() + "] must be a non-empty field name");
        }
        if (k < 1 || k > MAX_K) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be between 1 and " + MAX_K);
        }
        if (maxQueriesPerBatch < 1 || maxQueriesPerBatch > MAX_QUERIES_PER_BATCH) {
            throw new IllegalArgumentException(
                "[" + MAX_QUERIES_PER_BATCH_FIELD.getPreferredName() + "] must be between 1 and " + MAX_QUERIES_PER_BATCH
            );
        }
    }

    private static void validateQuerySource(@Nullable List<KnnEvalQuery> queries, @Nullable KnnEvalSample sample) {
        if ((queries == null) == (sample == null)) {
            throw new IllegalArgumentException(
                "exactly one of [" + QUERIES_FIELD.getPreferredName() + "] and [" + SAMPLE_FIELD.getPreferredName() + "] must be provided"
            );
        }
        if (queries != null) {
            if (queries.isEmpty()) {
                throw new IllegalArgumentException("[" + QUERIES_FIELD.getPreferredName() + "] must not be empty");
            }
            if (queries.size() > MAX_QUERIES) {
                throw new IllegalArgumentException(
                    "[" + QUERIES_FIELD.getPreferredName() + "] must contain at most " + MAX_QUERIES + " entries"
                );
            }
            Set<String> ids = new HashSet<>();
            for (KnnEvalQuery query : queries) {
                if (ids.add(query.getId()) == false) {
                    throw new IllegalArgumentException("duplicate query id [" + query.getId() + "]");
                }
            }
        }
    }

    private static KnnEvalKnobs normalizeBaseline(KnnEvalKnobs baseline) {
        Objects.requireNonNull(baseline, "[" + BASELINE_FIELD.getPreferredName() + "] must not be null");
        if (baseline.isExact() == false
            && baseline.getVisitPercentage() == null
            && baseline.getNumCandidates() == null
            && baseline.getRescoreOversample() == null) {
            return DEFAULT_BASELINE;
        }
        return baseline;
    }

    private static void validateCandidates(List<KnnEvalKnobs> knnSettings, int k) {
        if (knnSettings == null || knnSettings.isEmpty() || knnSettings.size() > MAX_KNN_SETTINGS) {
            throw new IllegalArgumentException(
                "[" + KNN_SETTINGS_FIELD.getPreferredName() + "] must contain between 1 and " + MAX_KNN_SETTINGS + " entries"
            );
        }
        Set<KnnEvalKnobs> uniqueCandidates = new HashSet<>();
        for (KnnEvalKnobs candidate : knnSettings) {
            validateNumCandidates(candidate, k);
            if (uniqueCandidates.add(candidate) == false) {
                throw new IllegalArgumentException("duplicate entry in [" + KNN_SETTINGS_FIELD.getPreferredName() + "]: " + candidate);
            }
            if (candidate.isExact()) {
                // it would be measuring the reference against itself
                throw new IllegalArgumentException(
                    "[" + KnnEvalKnobs.EXACT_FIELD.getPreferredName() + "] is only supported on the baseline, not in [knn_settings]"
                );
            }
        }
    }

    /** The kNN query rejects this too, but here the error can name the offending knob set rather than one failed query. */
    private static void validateNumCandidates(KnnEvalKnobs knobs, int k) {
        Integer numCandidates = knobs.getNumCandidates();
        if (numCandidates != null && numCandidates < k) {
            throw new IllegalArgumentException(
                "["
                    + KnnEvalKnobs.NUM_CANDIDATES_FIELD.getPreferredName()
                    + "] cannot be less than ["
                    + K_FIELD.getPreferredName()
                    + "] in "
                    + knobs
            );
        }
        if (numCandidates != null && numCandidates > KnnEvalRescore.MAX_NUM_CANDIDATES) {
            throw new IllegalArgumentException(
                "["
                    + KnnEvalKnobs.NUM_CANDIDATES_FIELD.getPreferredName()
                    + "] cannot exceed "
                    + KnnEvalRescore.MAX_NUM_CANDIDATES
                    + " in "
                    + knobs
            );
        }
    }

    KnnEvalSpec(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readVInt(),
            in.readOptionalCollectionAsList(KnnEvalQuery::new),
            in.readOptionalWriteable(KnnEvalSample::new),
            new KnnEvalKnobs(in),
            in.readCollectionAsList(KnnEvalKnobs::new),
            in.readVInt()
        );
    }

    /** Parses and validates an evaluation specification. */
    public static KnnEvalSpec parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String getField() {
        return field;
    }

    public int getK() {
        return k;
    }

    /** The caller-supplied query set, or {@code null} when the queries are {@link #getSample() sampled} server-side. */
    @Nullable
    public List<KnnEvalQuery> getQueries() {
        return queries;
    }

    /** The server-side sampling request, or {@code null} when the caller supplied {@link #getQueries() queries} directly. */
    @Nullable
    public KnnEvalSample getSample() {
        return sample;
    }

    /** Never {@code null}: an omitted baseline uses the bounded DiskBBQ proxy. */
    public KnnEvalKnobs getBaseline() {
        return baseline;
    }

    /** The knob sets being measured against the baseline, in request order. */
    public List<KnnEvalKnobs> getKnnSettings() {
        return knnSettings;
    }

    /** Batches run strictly one after another, so this trades wall-clock time against coordinator heap. */
    public int getMaxQueriesPerBatch() {
        return maxQueriesPerBatch;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(k);
        out.writeOptionalCollection(queries);
        out.writeOptionalWriteable(sample);
        baseline.writeTo(out);
        out.writeCollection(knnSettings);
        out.writeVInt(maxQueriesPerBatch);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(K_FIELD.getPreferredName(), k);
        if (queries != null) {
            builder.startArray(QUERIES_FIELD.getPreferredName());
            for (KnnEvalQuery query : queries) {
                query.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (sample != null) {
            builder.field(SAMPLE_FIELD.getPreferredName());
            sample.toXContent(builder, params);
        }
        builder.field(BASELINE_FIELD.getPreferredName());
        baseline.toXContent(builder, params);
        builder.startArray(KNN_SETTINGS_FIELD.getPreferredName());
        for (KnnEvalKnobs candidate : knnSettings) {
            candidate.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(MAX_QUERIES_PER_BATCH_FIELD.getPreferredName(), maxQueriesPerBatch);
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
        KnnEvalSpec other = (KnnEvalSpec) obj;
        return k == other.k
            && maxQueriesPerBatch == other.maxQueriesPerBatch
            && Objects.equals(field, other.field)
            && Objects.equals(queries, other.queries)
            && Objects.equals(sample, other.sample)
            && Objects.equals(baseline, other.baseline)
            && Objects.equals(knnSettings, other.knnSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, queries, sample, baseline, knnSettings, maxQueriesPerBatch);
    }
}
