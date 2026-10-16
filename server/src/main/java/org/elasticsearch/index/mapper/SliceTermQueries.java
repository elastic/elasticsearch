/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.UTF32ToUTF8;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Search-time counterpart of {@link SliceTermPrefix}: a slice index prefixes the terms in its dictionary with the slice
 * they belong to, so a query has to ask for the same thing.
 *
 * <p>Field types build their own queries, so applying the prefix there means every query shape goes through it, the way
 * {@code _id} is handled for slice indices. It also keeps the resulting query honest about what it matches, which
 * matters because the query cache keys on the query and a plain term would be the same in every slice.
 */
public final class SliceTermQueries {

    private SliceTermQueries() {}

    /** How a search sees a field's terms. */
    public enum Scope {
        /** Terms carry no slice, either because the index has none or because the field is not prefixed. */
        PLAIN,
        /** Terms carry a slice and the search targets specific ones. */
        SLICES,
        /** Terms carry a slice and the search spans all of them. */
        ALL_SLICES
    }

    /** How the terms of {@code field} must be asked for on the index this search targets. */
    public static Scope scope(String field, @Nullable SearchExecutionContext context) {
        if (context == null || SliceIndexing.prefixesTerms(field) == false) {
            return Scope.PLAIN;
        }
        final var settings = context.getIndexSettings();
        if (settings == null || settings.isSliceEnabled() == false) {
            return Scope.PLAIN;
        }
        return context.sliceSelection().isRestricted() ? Scope.SLICES : Scope.ALL_SLICES;
    }

    /** The slices a search targets. Never empty, because {@link Scope#SLICES} means the search named them. */
    private static List<String> slices(SearchExecutionContext context) {
        return context.sliceSelection().names();
    }

    /**
     * A search across every slice cannot be answered by a shape that works one term at a time: the slices of an index
     * cannot be enumerated, so there is nothing to ask each of them for. {@code reason} says what the shape needs.
     */
    private static IllegalArgumentException cannotSpanEverySlice(String field, String reason) {
        return new IllegalArgumentException(
            "["
                + SliceIndexing.FIELD_NAME
                + "="
                + SliceIndexing.SLICE_ALL
                + "] is not supported for this query on field ["
                + field
                + "]; "
                + reason
        );
    }

    /**
     * {@code build} applied to each slice the search targets, combined by {@code combine} when there is more than one.
     * A document belongs to one slice, so at most one of the parts can match it.
     */
    private static <T> T perSlice(List<String> slices, Function<String, T> build, Function<List<T>, T> combine) {
        if (slices.size() == 1) {
            return build.apply(slices.get(0));
        }
        final List<T> parts = new ArrayList<>(slices.size());
        for (String slice : slices) {
            parts.add(build.apply(slice));
        }
        return combine.apply(parts);
    }

    private static Query disjunction(List<Query> parts) {
        final BooleanQuery.Builder any = new BooleanQuery.Builder();
        for (Query part : parts) {
            any.add(part, BooleanClause.Occur.SHOULD);
        }
        return any.build();
    }

    /**
     * The one way a query shape becomes slice-aware. A shape says how it looks on a plain index and what automaton it
     * accepts; on a slice index the automaton is confined to the slices the search targets, and on a plain one the
     * shape is built as it always was.
     *
     * <p>Every shape in {@link StringFieldType} and {@link TermBasedFieldType} goes through here, so slices are one
     * line in each rather than a branch to remember. {@code SliceTermsVerifier} fails any shape that forgets, and any
     * shape whose {@code accepts} is not over the bytes of a term; {@link #binary} converts one that is not.
     */
    public static Query shape(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<Query> onPlainIndex,
        Supplier<Automaton> accepts,
        @Nullable MultiTermQuery.RewriteMethod method
    ) {
        return shape(field, context, onPlainIndex, null, accepts, method);
    }

    /**
     * As above, for a shape that has a native form once it knows which slice it is asking about: {@code onSlice} is given
     * the slice and returns the shape's own query over the prefixed term, keeping whatever that query type does better
     * than a bare automaton. {@link org.apache.lucene.search.FuzzyQuery} is the case in point, where pinning the slice
     * with {@code prefixLength} keeps its Levenshtein enumeration and its scoring.
     *
     * <p>Searching every slice still goes through the automaton, since no single term can say "any slice".
     */
    public static Query shape(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<Query> onPlainIndex,
        @Nullable Function<String, Query> onSlice,
        Supplier<Automaton> accepts,
        @Nullable MultiTermQuery.RewriteMethod method
    ) {
        return switch (scope(field, context)) {
            case PLAIN -> onPlainIndex.get();
            case SLICES -> onSlice == null
                ? automatonQuery(field, accepts.get(), method, context)
                : perSlice(slices(context), onSlice, SliceTermQueries::disjunction);
            case ALL_SLICES -> automatonQuery(field, accepts.get(), method, context);
        };
    }

    /** The exclusive end of a slice's range in the dictionary, for a range left open at the top. */
    public static BytesRef sliceEnd(String slice) {
        final byte[] end = SliceIndexing.termPrefixBytes(slice);
        end[end.length - 1]++;
        return new BytesRef(end);
    }

    /**
     * An automaton written in code points as the bytes of a term. The dictionary is bytes and the slice is put in front
     * of an automaton byte for byte, so a shape whose automaton is written in characters converts it here instead of
     * leaving it to the query it would otherwise have become.
     */
    public static Automaton binary(Automaton codePoints) {
        return new UTF32ToUTF8().convert(codePoints);
    }

    /** The form {@code value} takes in the dictionary of {@code slice}. */
    public static BytesRef term(String slice, BytesRef value) {
        return SliceIndexing.prefixTerm(slice, value);
    }

    /** A query for one term, asking each targeted slice for its own copy. */
    public static Query termQuery(String field, BytesRef value, @Nullable SearchExecutionContext context) {
        return switch (scope(field, context)) {
            case PLAIN -> new TermQuery(new Term(field, value));
            case SLICES -> {
                final List<String> slices = slices(context);
                yield slices.size() == 1
                    ? new TermQuery(new Term(field, term(slices.get(0), value)))
                    : termsQuery(field, List.of(value), context);
            }
            case ALL_SLICES -> anySlice(field, Automata.makeBinary(value));
        };
    }

    /** A query for several terms, asking each targeted slice for its own copy of each. */
    public static Query termsQuery(String field, Collection<BytesRef> values, @Nullable SearchExecutionContext context) {
        return switch (scope(field, context)) {
            case PLAIN -> new TermInSetQuery(field, values);
            case SLICES -> {
                final List<BytesRef> terms = new ArrayList<>(values.size() * slices(context).size());
                for (String slice : slices(context)) {
                    for (BytesRef value : values) {
                        terms.add(term(slice, value));
                    }
                }
                yield new TermInSetQuery(field, terms);
            }
            case ALL_SLICES -> {
                final List<BytesRef> sorted = new ArrayList<>(values);
                sorted.sort(BytesRef::compareTo);
                yield anySlice(field, Automata.makeBinaryStringUnion(sorted));
            }
        };
    }

    /**
     * Shapes a query that can only ever name one term, such as the ones {@link org.apache.lucene.document.FeatureField}
     * builds for {@code sparse_vector} and {@code rank_features}: {@code build} is handed the term to look up, once for
     * each targeted slice, and the results are combined into a disjunction. A document belongs to one slice, so at most
     * one clause can match it and its score is the one that clause gives it.
     *
     * <p>A search spanning every slice is declined. These shapes carry their own scoring and have no multi-term form to
     * confine with an automaton, and the slices of an index cannot be enumerated to disjoin over.
     */
    public static Query singleTermShape(
        String field,
        String term,
        @Nullable SearchExecutionContext context,
        Function<String, Query> build
    ) {
        return switch (scope(field, context)) {
            case PLAIN -> build.apply(term);
            case SLICES -> perSlice(
                slices(context),
                slice -> build.apply(SliceIndexing.termPrefix(slice) + term),
                SliceTermQueries::disjunction
            );
            case ALL_SLICES -> throw cannotSpanEverySlice(field, "it scores one term at a time, so a single slice must be named");
        };
    }

    /**
     * The term {@code value} is looked up under in each targeted slice, for a statistic a query gathers itself rather
     * than through a shape.
     */
    public static List<String> statisticTerms(String field, String value, @Nullable SearchExecutionContext context) {
        return switch (scope(field, context)) {
            case PLAIN -> List.of(value);
            case SLICES -> {
                final List<String> terms = new ArrayList<>();
                for (String slice : slices(context)) {
                    terms.add(SliceIndexing.termPrefix(slice) + value);
                }
                yield terms;
            }
            case ALL_SLICES -> throw cannotSpanEverySlice(
                field,
                "it reads a statistic one term at a time, so a single slice must be named"
            );
        };
    }

    /**
     * Confines an automaton built over plain terms to the slices a search targets. Every multi-term shape goes through
     * here when it cannot express itself as a single term.
     */
    public static Query automatonQuery(
        String field,
        Automaton plain,
        @Nullable MultiTermQuery.RewriteMethod method,
        @Nullable SearchExecutionContext context
    ) {
        return switch (scope(field, context)) {
            case PLAIN -> automaton(field, plain, method);
            case SLICES -> perSlice(
                slices(context),
                slice -> automaton(field, confineTo(slice, plain), method),
                SliceTermQueries::disjunction
            );
            case ALL_SLICES -> automaton(field, confineToAny(plain), method);
        };
    }

    /**
     * The intervals counterpart of {@link #shape}: a shape says how it looks on a plain index and what it accepts, and
     * on a slice index it is confined to the slices the search targets. Every interval shape in a text field goes
     * through here.
     */
    public static IntervalsSource intervals(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<IntervalsSource> onPlainIndex,
        Supplier<Automaton> accepts,
        String label
    ) {
        return intervals(field, context, onPlainIndex, null, accepts, label);
    }

    /**
     * As above, for a shape with a native source once it knows the slice: {@code onSlice} returns the shape's own source
     * over the prefixed term, which for a plain term is a direct lookup rather than a term enumeration.
     */
    public static IntervalsSource intervals(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<IntervalsSource> onPlainIndex,
        @Nullable Function<String, IntervalsSource> onSlice,
        Supplier<Automaton> accepts,
        String label
    ) {
        return switch (scope(field, context)) {
            case PLAIN -> onPlainIndex.get();
            case SLICES -> perSlice(
                slices(context),
                onSlice != null ? onSlice : slice -> confined(confineTo(slice, accepts.get()), label),
                Intervals::or
            );
            case ALL_SLICES -> confined(confineToAny(accepts.get()), label);
        };
    }

    private static IntervalsSource confined(Automaton automaton, String label) {
        return Intervals.multiterm(compile(automaton), IndexSearcher.getMaxClauseCount(), label);
    }

    private static CompiledAutomaton compile(Automaton automaton) {
        return new CompiledAutomaton(automaton, false, false, true);
    }

    /** {@code plain} in the dictionary of one slice: that slice's prefix followed by whatever {@code plain} accepts. */
    private static Automaton confineTo(String slice, Automaton plain) {
        return determinize(Operations.concatenate(List.of(Automata.makeBinary(new BytesRef(SliceIndexing.termPrefixBytes(slice))), plain)));
    }

    /** Any slice's prefix followed by {@code plain}: {@code ANY^TERM_PREFIX_LENGTH plain}. */
    private static Automaton confineToAny(Automaton plain) {
        return determinize(Operations.concatenate(List.of(anySlicePrefix(), plain)));
    }

    /** Exactly {@link SliceIndexing#TERM_PREFIX_LENGTH} arbitrary bytes, which is what any slice's prefix looks like. */
    public static Automaton anySlicePrefix() {
        final Automaton any = new Automaton();
        int from = any.createState();
        for (int i = 0; i < SliceIndexing.TERM_PREFIX_LENGTH; i++) {
            final int to = any.createState();
            any.addTransition(from, to, 0, 0xFF);
            from = to;
        }
        any.setAccept(from, true);
        any.finishState();
        return any;
    }

    private static Automaton determinize(Automaton automaton) {
        // Concatenation yields a non-deterministic automaton, and a query may not run one.
        return Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    private static Query anySlice(String field, Automaton plain) {
        return automaton(field, confineToAny(plain), null);
    }

    private static AutomatonQuery automaton(String field, Automaton automaton, @Nullable MultiTermQuery.RewriteMethod method) {
        return method == null
            ? new AutomatonQuery(new Term(field), automaton, true)
            : new AutomatonQuery(new Term(field), automaton, true, method);
    }
}
