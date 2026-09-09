/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
 * Specification of a kNN recall-estimation request.
 * <p>
 * Brute-force ground truth for a large vector index is expensive enough that people simply do not measure ANN recall. This spec encodes
 * a cheaper substitute: run the same query vectors against the same field twice, once with a generous {@code visit_percentage} (the
 * {@link #getBaseline() baseline}, treated as a stand-in for ground truth) and once per cheaper {@link #getCandidates() candidate}, and
 * report how much of the baseline's top-k each candidate recovered. A caller sweeps several candidates against one baseline to plot a
 * recall-vs-cost curve.
 * <p>
 * Because the two runs differ only in the {@link KnnEvalKnobs knobs}, the query set itself has to be fixed for the whole request --
 * hence exactly one of {@link #getQueries() queries} (caller-supplied vectors) or {@link #getSample() sample} (vectors drawn
 * server-side from the indexed documents) must be given. An optional {@link #getFilter() filter} likewise applies to both runs.
 */
public class KnnEvalSpec implements Writeable, ToXContentObject {

    static final ParseField FIELD_FIELD = new ParseField("field");
    static final ParseField K_FIELD = new ParseField("k");
    static final ParseField QUERIES_FIELD = new ParseField("queries");
    static final ParseField SAMPLE_FIELD = new ParseField("sample");
    static final ParseField BASELINE_FIELD = new ParseField("baseline");
    static final ParseField CANDIDATES_FIELD = new ParseField("candidates");
    static final ParseField INCLUDE_DETAILS_FIELD = new ParseField("include_details");
    static final ParseField FILTER_FIELD = new ParseField("filter");
    static final ParseField MAX_QUERIES_PER_BATCH_FIELD = new ParseField("max_queries_per_batch");
    static final ParseField MAX_CONCURRENT_SEARCHES_FIELD = new ParseField("max_concurrent_searches");
    static final ParseField INCLUDE_FIDELITY_FIELD = new ParseField("include_fidelity");
    static final ParseField VALUE_TOLERANCE_FIELD = new ParseField("value_tolerance");

    private static final boolean DEFAULT_INCLUDE_DETAILS = false;

    /**
     * How many queries go into one {@code _msearch} by default. A coordinator buffers every sub-search response of an msearch until the
     * last one lands, so fan-out is what bounds heap here, not concurrency: at 7 knob sets and {@code k = 100} a single unbatched
     * request of 2000 queries produced 14000 buffered responses and exhausted a 4 GB heap. 50 queries keeps one batch to a few hundred
     * sub-searches for realistic sweeps.
     */
    /**
     * With no baseline given, the reference is exact search. Recall against anything else is recall against a proxy, and a caller who
     * did not say otherwise wants the real number; a cheaper baseline is an optimisation to opt into once it has been certified.
     */
    private static final KnnEvalKnobs DEFAULT_BASELINE = new KnnEvalKnobs(null, null, null, true);

    private static final int DEFAULT_MAX_QUERIES_PER_BATCH = 50;

    /**
     * Searches within a batch run one at a time by default. Concurrency inside a batch contaminates the reported {@code took}: on a
     * busy or small node the searches contend for the search thread pool, and a candidate scheduled alongside the expensive baseline
     * absorbs that contention. Reversing the candidate order was observed to move a candidate's {@code took} by two orders of
     * magnitude while its {@code vector_ops} stayed identical.
     */
    private static final int DEFAULT_MAX_CONCURRENT_SEARCHES = 1;

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
            args[6] == null ? DEFAULT_INCLUDE_DETAILS : (Boolean) args[6],
            (QueryBuilder) args[7],
            args[8] == null ? DEFAULT_MAX_QUERIES_PER_BATCH : (Integer) args[8],
            args[9] == null ? DEFAULT_MAX_CONCURRENT_SEARCHES : (Integer) args[9],
            args[10] != null && (Boolean) args[10],
            (Double) args[11]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), K_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalQuery.fromXContent(p), QUERIES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalSample.fromXContent(p), SAMPLE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> KnnEvalKnobs.fromXContent(p), BASELINE_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> KnnEvalKnobs.fromXContent(p), CANDIDATES_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), INCLUDE_DETAILS_FIELD);
        // same shape as the `filter` inside a `knn` search section
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER_FIELD
        );
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_QUERIES_PER_BATCH_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_CONCURRENT_SEARCHES_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), INCLUDE_FIDELITY_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.optionalConstructorArg(), VALUE_TOLERANCE_FIELD);
    }

    private final String field;
    private final int k;
    @Nullable
    private final List<KnnEvalQuery> queries;
    @Nullable
    private final KnnEvalSample sample;
    private final KnnEvalKnobs baseline;
    private final List<KnnEvalKnobs> candidates;
    private final boolean includeDetails;
    @Nullable
    private final QueryBuilder filter;
    private final int maxQueriesPerBatch;
    private final int maxConcurrentSearches;
    private final boolean includeFidelity;
    private final double valueTolerance;

    public KnnEvalSpec(
        String field,
        int k,
        @Nullable List<KnnEvalQuery> queries,
        @Nullable KnnEvalSample sample,
        KnnEvalKnobs baseline,
        List<KnnEvalKnobs> candidates,
        boolean includeDetails,
        @Nullable QueryBuilder filter,
        int maxQueriesPerBatch,
        int maxConcurrentSearches,
        boolean includeFidelity,
        @Nullable Double valueTolerance
    ) {
        if (Strings.hasText(field) == false) {
            throw new IllegalArgumentException("[" + FIELD_FIELD.getPreferredName() + "] must be a non-empty field name");
        }
        if (k < 1) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (maxQueriesPerBatch < 1) {
            throw new IllegalArgumentException("[" + MAX_QUERIES_PER_BATCH_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (maxConcurrentSearches < 1) {
            throw new IllegalArgumentException("[" + MAX_CONCURRENT_SEARCHES_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (valueTolerance != null && includeFidelity == false) {
            // Silently ignoring it would leave a caller believing they had asked for something.
            throw new IllegalArgumentException(
                "[" + VALUE_TOLERANCE_FIELD.getPreferredName() + "] requires [" + INCLUDE_FIDELITY_FIELD.getPreferredName() + "] to be true"
            );
        }
        if (valueTolerance != null && valueTolerance < 0.0) {
            throw new IllegalArgumentException("[" + VALUE_TOLERANCE_FIELD.getPreferredName() + "] must not be negative");
        }
        if ((queries == null) == (sample == null)) {
            throw new IllegalArgumentException(
                "exactly one of [" + QUERIES_FIELD.getPreferredName() + "] and [" + SAMPLE_FIELD.getPreferredName() + "] must be provided"
            );
        }
        if (queries != null) {
            if (queries.isEmpty()) {
                throw new IllegalArgumentException("[" + QUERIES_FIELD.getPreferredName() + "] must not be empty");
            }
            Set<String> ids = new HashSet<>();
            for (KnnEvalQuery query : queries) {
                if (ids.add(query.getId()) == false) {
                    throw new IllegalArgumentException("duplicate query id [" + query.getId() + "]");
                }
            }
        }
        Objects.requireNonNull(baseline, "[" + BASELINE_FIELD.getPreferredName() + "] must not be null");
        if (candidates == null || candidates.isEmpty()) {
            throw new IllegalArgumentException("[" + CANDIDATES_FIELD.getPreferredName() + "] must not be empty");
        }
        validateNumCandidates(baseline, k);
        if (baseline.isExact()
            && (baseline.getVisitPercentage() != null || baseline.getNumCandidates() != null || baseline.getOversample() != null)) {
            throw new IllegalArgumentException(
                "["
                    + KnnEvalKnobs.EXACT_FIELD.getPreferredName()
                    + "] cannot be combined with ["
                    + KnnEvalKnobs.VISIT_PERCENTAGE_FIELD.getPreferredName()
                    + "], ["
                    + KnnEvalKnobs.NUM_CANDIDATES_FIELD.getPreferredName()
                    + "] or ["
                    + KnnEvalKnobs.OVERSAMPLE_FIELD.getPreferredName()
                    + "]"
            );
        }
        for (KnnEvalKnobs candidate : candidates) {
            validateNumCandidates(candidate, k);
            if (candidate.isExact()) {
                // An exact candidate would be measuring the reference against itself; exactness is what a baseline is for.
                throw new IllegalArgumentException(
                    "[" + KnnEvalKnobs.EXACT_FIELD.getPreferredName() + "] is only supported on the baseline"
                );
            }
        }
        this.field = field;
        this.k = k;
        this.queries = queries == null ? null : List.copyOf(queries);
        this.sample = sample;
        this.baseline = baseline;
        this.candidates = List.copyOf(candidates);
        this.includeDetails = includeDetails;
        this.filter = filter;
        this.maxQueriesPerBatch = maxQueriesPerBatch;
        this.maxConcurrentSearches = maxConcurrentSearches;
        this.includeFidelity = includeFidelity;
        this.valueTolerance = valueTolerance == null ? 0.0 : valueTolerance;
    }

    /**
     * The underlying kNN query rejects a {@code num_candidates} below {@code k}, so reject it here where the error can name the offending
     * knobs object rather than surfacing as a per-query search failure.
     */
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
    }

    KnnEvalSpec(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readVInt(),
            in.readOptionalCollectionAsList(KnnEvalQuery::new),
            in.readOptionalWriteable(KnnEvalSample::new),
            new KnnEvalKnobs(in),
            in.readCollectionAsList(KnnEvalKnobs::new),
            in.readBoolean(),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readVInt(),
            in.readVInt(),
            in.readBoolean(),
            // sent as optional so that the flag-off case round trips through the constructor's own validation
            in.readOptionalDouble()
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

    /** Never {@code null}: an omitted baseline defaults to exact search. */
    public KnnEvalKnobs getBaseline() {
        return baseline;
    }

    public List<KnnEvalKnobs> getCandidates() {
        return candidates;
    }

    public boolean isIncludeDetails() {
        return includeDetails;
    }

    /**
     * A filter applied identically to the baseline and every candidate search, so that recall is measured under the same restriction
     * the production query would run under. It is deliberately <em>not</em> applied to the sampling search: query vectors are drawn from
     * the whole corpus so that recall under a restrictive filter is measurable rather than being masked by a matching query set.
     */
    @Nullable
    public QueryBuilder getFilter() {
        return filter;
    }

    /**
     * The number of queries whose searches are submitted together. Batches run strictly one after another, so this is the knob that
     * trades wall-clock time against coordinator heap.
     */
    public int getMaxQueriesPerBatch() {
        return maxQueriesPerBatch;
    }

    /**
     * How many of a batch's searches may run at once. The default of 1 serializes them so that each reported {@code took} is one
     * search's shard time rather than a figure inflated by contention with its siblings. Raise it to finish a sweep sooner when only
     * the recall numbers, not the latencies, are wanted.
     */
    public int getMaxConcurrentSearches() {
        return maxConcurrentSearches;
    }

    /**
     * Whether to compute the value-based metrics ({@code recall_value} and profile fidelity). They exist for internal analysis, need the
     * field mapping to invert {@code _score} back into a similarity, and would otherwise clutter a response whose headline number is
     * recall, so they are off unless asked for.
     */
    public boolean isIncludeFidelity() {
        return includeFidelity;
    }

    /**
     * Multiplicative slack for the value-based recall: a candidate hit counts as a match when its similarity is within this fraction of
     * the worst similarity the baseline accepted. Zero means "at least as good as the baseline's k-th hit". It has no effect on the
     * id-based recall.
     */
    public double getValueTolerance() {
        return valueTolerance;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(k);
        out.writeOptionalCollection(queries);
        out.writeOptionalWriteable(sample);
        baseline.writeTo(out);
        out.writeCollection(candidates);
        out.writeBoolean(includeDetails);
        out.writeOptionalNamedWriteable(filter);
        out.writeVInt(maxQueriesPerBatch);
        out.writeVInt(maxConcurrentSearches);
        out.writeBoolean(includeFidelity);
        out.writeOptionalDouble(includeFidelity ? valueTolerance : null);
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
        builder.startArray(CANDIDATES_FIELD.getPreferredName());
        for (KnnEvalKnobs candidate : candidates) {
            candidate.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(INCLUDE_DETAILS_FIELD.getPreferredName(), includeDetails);
        if (filter != null) {
            builder.field(FILTER_FIELD.getPreferredName(), filter);
        }
        builder.field(MAX_QUERIES_PER_BATCH_FIELD.getPreferredName(), maxQueriesPerBatch);
        builder.field(MAX_CONCURRENT_SEARCHES_FIELD.getPreferredName(), maxConcurrentSearches);
        builder.field(INCLUDE_FIDELITY_FIELD.getPreferredName(), includeFidelity);
        if (includeFidelity) {
            builder.field(VALUE_TOLERANCE_FIELD.getPreferredName(), valueTolerance);
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
        KnnEvalSpec other = (KnnEvalSpec) obj;
        return k == other.k
            && includeDetails == other.includeDetails
            && maxQueriesPerBatch == other.maxQueriesPerBatch
            && maxConcurrentSearches == other.maxConcurrentSearches
            && includeFidelity == other.includeFidelity
            && Double.compare(valueTolerance, other.valueTolerance) == 0
            && Objects.equals(field, other.field)
            && Objects.equals(queries, other.queries)
            && Objects.equals(sample, other.sample)
            && Objects.equals(baseline, other.baseline)
            && Objects.equals(candidates, other.candidates)
            && Objects.equals(filter, other.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field,
            k,
            queries,
            sample,
            baseline,
            candidates,
            includeDetails,
            filter,
            maxQueriesPerBatch,
            maxConcurrentSearches,
            includeFidelity,
            valueTolerance
        );
    }
}
