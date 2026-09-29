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

/** One recall-versus-cost evaluation: queries or a sample, a baseline, and the settings measured against it. */
final class KnnEvalSpec implements Writeable, ToXContentObject {

    static final int MAX_QUERIES = 1_000;
    static final int MAX_K = 1_000;
    static final int MAX_KNN_SETTINGS = 32;

    static final ParseField FIELD_FIELD = new ParseField("field");
    static final ParseField K_FIELD = new ParseField("k");
    static final ParseField QUERIES_FIELD = new ParseField("queries");
    static final ParseField SAMPLE_FIELD = new ParseField("sample");
    static final ParseField BASELINE_FIELD = new ParseField("baseline");
    static final ParseField KNN_SETTINGS_FIELD = new ParseField("knn_settings");

    /** Bounded so an omitted baseline cannot unexpectedly scan every full-precision vector; exact stays available explicitly. */
    private static final KnnEvalSettings DEFAULT_BASELINE = new KnnEvalSettings(20.0f, null, 100.0f, false);

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KnnEvalSpec, Void> PARSER = new ConstructingObjectParser<>(
        "knn_eval",
        args -> new KnnEvalSpec(
            (String) args[0],
            (Integer) args[1],
            (List<KnnEvalQuery>) args[2],
            (KnnEvalSample) args[3],
            args[4] == null ? DEFAULT_BASELINE : (KnnEvalSettings) args[4],
            (List<KnnEvalSettings>) args[5]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), K_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalQuery.fromXContent(p), QUERIES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalSample.fromXContent(p), SAMPLE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalSettings.fromXContent(p), BASELINE_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> KnnEvalSettings.fromXContent(p), KNN_SETTINGS_FIELD);
    }

    private final String field;
    private final int k;
    @Nullable
    private final List<KnnEvalQuery> queries;
    @Nullable
    private final KnnEvalSample sample;
    private final KnnEvalSettings baseline;
    private final List<KnnEvalSettings> knnSettings;

    KnnEvalSpec(
        String field,
        int k,
        @Nullable List<KnnEvalQuery> queries,
        @Nullable KnnEvalSample sample,
        KnnEvalSettings baseline,
        List<KnnEvalSettings> knnSettings
    ) {
        validateBounds(field, k);
        validateQuerySource(queries, sample);
        baseline = normalizeBaseline(baseline);
        // a sampled query also retrieves its own document, so it searches one extra candidate
        int maxNumCandidates = sample == null ? KnnEvalRescore.MAX_NUM_CANDIDATES : KnnEvalRescore.MAX_NUM_CANDIDATES - 1;
        validateNumCandidates(baseline, k, maxNumCandidates);
        validateCandidates(knnSettings, k, maxNumCandidates);
        this.field = field;
        this.k = k;
        this.queries = queries == null ? null : List.copyOf(queries);
        this.sample = sample;
        this.baseline = baseline;
        this.knnSettings = List.copyOf(knnSettings);
    }

    private static void validateBounds(String field, int k) {
        if (Strings.hasText(field) == false) {
            throw new IllegalArgumentException("[" + FIELD_FIELD.getPreferredName() + "] must be a non-empty field name");
        }
        if (k < 1 || k > MAX_K) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be between 1 and " + MAX_K);
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

    private static KnnEvalSettings normalizeBaseline(KnnEvalSettings baseline) {
        Objects.requireNonNull(baseline, "[" + BASELINE_FIELD.getPreferredName() + "] must not be null");
        if (baseline.isExact() == false
            && baseline.getVisitPercentage() == null
            && baseline.getNumCandidates() == null
            && baseline.getRescoreOversample() == null) {
            return DEFAULT_BASELINE;
        }
        return baseline;
    }

    private static void validateCandidates(List<KnnEvalSettings> knnSettings, int k, int maxNumCandidates) {
        if (knnSettings == null || knnSettings.isEmpty() || knnSettings.size() > MAX_KNN_SETTINGS) {
            throw new IllegalArgumentException(
                "[" + KNN_SETTINGS_FIELD.getPreferredName() + "] must contain between 1 and " + MAX_KNN_SETTINGS + " entries"
            );
        }
        Set<KnnEvalSettings> uniqueCandidates = new HashSet<>();
        for (KnnEvalSettings candidate : knnSettings) {
            validateNumCandidates(candidate, k, maxNumCandidates);
            if (uniqueCandidates.add(candidate) == false) {
                throw new IllegalArgumentException("duplicate entry in [" + KNN_SETTINGS_FIELD.getPreferredName() + "]: " + candidate);
            }
            if (candidate.isExact()) {
                // it would be measuring the reference against itself
                throw new IllegalArgumentException(
                    "[" + KnnEvalSettings.EXACT_FIELD.getPreferredName() + "] is only supported on the baseline, not in [knn_settings]"
                );
            }
        }
    }

    /** The kNN query rejects this too, but here the error can name the offending settings entry rather than one failed query. */
    private static void validateNumCandidates(KnnEvalSettings knnSettings, int k, int maxNumCandidates) {
        Integer numCandidates = knnSettings.getNumCandidates();
        if (numCandidates != null && numCandidates < k) {
            throw new IllegalArgumentException(
                "["
                    + KnnEvalSettings.NUM_CANDIDATES_FIELD.getPreferredName()
                    + "] cannot be less than ["
                    + K_FIELD.getPreferredName()
                    + "] in "
                    + knnSettings
            );
        }
        if (numCandidates != null && numCandidates > maxNumCandidates) {
            throw new IllegalArgumentException(
                "["
                    + KnnEvalSettings.NUM_CANDIDATES_FIELD.getPreferredName()
                    + "] cannot exceed "
                    + maxNumCandidates
                    + (maxNumCandidates < KnnEvalRescore.MAX_NUM_CANDIDATES ? " with [" + SAMPLE_FIELD.getPreferredName() + "]" : "")
                    + " in "
                    + knnSettings
            );
        }
    }

    KnnEvalSpec(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readVInt(),
            in.readOptionalCollectionAsList(KnnEvalQuery::new),
            in.readOptionalWriteable(KnnEvalSample::new),
            new KnnEvalSettings(in),
            in.readCollectionAsList(KnnEvalSettings::new)
        );
    }

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
    public KnnEvalSettings getBaseline() {
        return baseline;
    }

    public List<KnnEvalSettings> getKnnSettings() {
        return knnSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(k);
        out.writeOptionalCollection(queries);
        out.writeOptionalWriteable(sample);
        baseline.writeTo(out);
        out.writeCollection(knnSettings);
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
        for (KnnEvalSettings candidate : knnSettings) {
            candidate.toXContent(builder, params);
        }
        builder.endArray();
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
            && Objects.equals(field, other.field)
            && Objects.equals(queries, other.queries)
            && Objects.equals(sample, other.sample)
            && Objects.equals(baseline, other.baseline)
            && Objects.equals(knnSettings, other.knnSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, queries, sample, baseline, knnSettings);
    }
}
