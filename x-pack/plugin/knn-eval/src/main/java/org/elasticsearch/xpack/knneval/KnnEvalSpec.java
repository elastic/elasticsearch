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

/** One recall-versus-cost evaluation: a query source, a baseline, and the settings measured against it. */
final class KnnEvalSpec implements Writeable, ToXContentObject {

    static final int MAX_QUERIES = 1_000;
    static final int MAX_K = 1_000;
    static final int MAX_KNN_SETTINGS = 32;

    static final ParseField FIELD_FIELD = new ParseField("field");
    static final ParseField K_FIELD = new ParseField("k");
    static final ParseField QUERY_SOURCE_FIELD = KnnEvalQuerySource.QUERY_SOURCE_FIELD;
    static final ParseField BASELINE_FIELD = new ParseField("baseline");
    static final ParseField KNN_SETTINGS_FIELD = new ParseField("knn_settings");

    /** Bounded, so an omitted baseline can't scan every vector; exact must be explicit. */
    private static final KnnEvalSettings DEFAULT_BASELINE = new KnnEvalSettings(20.0f, null, 100.0f, false);

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KnnEvalSpec, Void> PARSER = new ConstructingObjectParser<>(
        "knn_eval",
        args -> new KnnEvalSpec(
            (String) args[0],
            (Integer) args[1],
            (KnnEvalQuerySource) args[2],
            args[3] == null ? DEFAULT_BASELINE : (KnnEvalSettings) args[3],
            (List<KnnEvalSettings>) args[4]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), K_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> KnnEvalQuerySource.fromXContent(p), QUERY_SOURCE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalSettings.fromXContent(p), BASELINE_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> KnnEvalSettings.fromXContent(p), KNN_SETTINGS_FIELD);
    }

    private final String field;
    private final int k;
    private final KnnEvalQuerySource querySource;
    private final KnnEvalSettings baseline;
    private final List<KnnEvalSettings> knnSettings;

    KnnEvalSpec(String field, int k, KnnEvalQuerySource querySource, KnnEvalSettings baseline, List<KnnEvalSettings> knnSettings) {
        validateBounds(field, k);
        Objects.requireNonNull(querySource, "[" + QUERY_SOURCE_FIELD.getPreferredName() + "] must be provided");
        validateVectorsSource(querySource, k);
        baseline = normalizeBaseline(baseline);
        boolean sampling = querySource instanceof KnnEvalQuerySource.DocsSource;
        // a sampled query also retrieves its own document: one extra candidate
        int maxNumCandidates = sampling ? KnnEvalRescore.MAX_NUM_CANDIDATES - 1 : KnnEvalRescore.MAX_NUM_CANDIDATES;
        validateNumCandidates(baseline, k, maxNumCandidates, sampling);
        validateCandidates(knnSettings, k, maxNumCandidates, sampling);
        this.field = field;
        this.k = k;
        this.querySource = querySource;
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

    private static void validateVectorsSource(KnnEvalQuerySource querySource, int k) {
        if (querySource instanceof KnnEvalQuerySource.VectorsSource vs) {
            List<KnnEvalQuery> vectors = vs.vectors();
            if (vectors.isEmpty()) {
                throw new IllegalArgumentException("[" + KnnEvalQuerySource.VECTORS_FIELD.getPreferredName() + "] must not be empty");
            }
            if (vectors.size() > MAX_QUERIES) {
                throw new IllegalArgumentException(
                    "[" + KnnEvalQuerySource.VECTORS_FIELD.getPreferredName() + "] must contain at most " + MAX_QUERIES + " entries"
                );
            }
            Set<String> ids = new HashSet<>();
            for (KnnEvalQuery query : vectors) {
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

    private static void validateCandidates(List<KnnEvalSettings> knnSettings, int k, int maxNumCandidates, boolean sampling) {
        if (knnSettings == null || knnSettings.isEmpty() || knnSettings.size() > MAX_KNN_SETTINGS) {
            throw new IllegalArgumentException(
                "[" + KNN_SETTINGS_FIELD.getPreferredName() + "] must contain between 1 and " + MAX_KNN_SETTINGS + " entries"
            );
        }
        Set<KnnEvalSettings> uniqueCandidates = new HashSet<>();
        for (KnnEvalSettings candidate : knnSettings) {
            validateNumCandidates(candidate, k, maxNumCandidates, sampling);
            if (uniqueCandidates.add(candidate) == false) {
                throw new IllegalArgumentException("duplicate entry in [" + KNN_SETTINGS_FIELD.getPreferredName() + "]: " + candidate);
            }
            if (candidate.isExact()) {
                // would compare the reference with itself
                throw new IllegalArgumentException(
                    "[" + KnnEvalSettings.EXACT_FIELD.getPreferredName() + "] is only supported on the baseline, not in [knn_settings]"
                );
            }
        }
    }

    /** The kNN query rejects this too, but here the error names the settings entry. */
    private static void validateNumCandidates(KnnEvalSettings knnSettings, int k, int maxNumCandidates, boolean sampling) {
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
                    + (sampling ? " with sampling" : "")
                    + " in "
                    + knnSettings
            );
        }
    }

    KnnEvalSpec(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readVInt(),
            KnnEvalQuerySource.read(in),
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

    public KnnEvalQuerySource getQuerySource() {
        return querySource;
    }

    /** The supplied queries, or {@code null} when sampled. */
    @Nullable
    public List<KnnEvalQuery> getQueries() {
        return querySource instanceof KnnEvalQuerySource.VectorsSource vs ? vs.vectors() : null;
    }

    /** Sampling parameters, or {@code null} when queries are supplied. */
    @Nullable
    public KnnEvalSample getSample() {
        return querySource instanceof KnnEvalQuerySource.DocsSource ds ? ds.sample() : null;
    }

    /** Never {@code null}: an omitted baseline is the bounded default. */
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
        querySource.writeTo(out);
        baseline.writeTo(out);
        out.writeCollection(knnSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(QUERY_SOURCE_FIELD.getPreferredName());
        querySource.toXContent(builder, params);
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
            && Objects.equals(querySource, other.querySource)
            && Objects.equals(baseline, other.baseline)
            && Objects.equals(knnSettings, other.knnSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, querySource, baseline, knnSettings);
    }
}
