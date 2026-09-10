/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;

import java.io.IOException;
import java.util.Objects;

/**
 * Documents whose keyword column holds a value an automaton accepts, answered by the column rather than by
 * an inverted index. This is what a pattern comes to once nothing simpler will express it.
 *
 * <p>An automaton says nothing a column can bisect on, so every distinct value has to be run through it. What
 * the column's shape decides is how many that is: a dictionary column runs a term once and lets every value
 * naming it inherit the answer, a column of runs runs a run once, and a column of neither runs a value once.
 * That is already better than a scan of the values by the ratio of documents to distinct values, which on a
 * keyword column is most of them.
 *
 * <p>Better still is not needing the automaton. A pattern that names a whole value, a start of one, or a run
 * of bytes inside one is one of the shapes {@link ColumnarStringTermQuery} answers, two of which bisect a
 * column in term order rather than looking at its values at all. {@link #forWildcard} decides that where the
 * query is built, so what goes into the cache key is the cheap query rather than a pattern that has to be
 * recognised again on every rewrite.
 *
 * <p>A caller gates on the format, so a field reaching this has a column. It is not always handed over as
 * one: an updated field is read as an overlay of its layers, which is no column, and then the values are
 * read a document at a time like any binary doc values.
 */
public final class ColumnarStringAutomatonQuery extends Query {

    private final String field;
    private final ByteRunAutomaton automaton;
    private final String description;
    private final ScanBudget budget;

    /**
     * Documents whose value {@code automaton} accepts.
     *
     * <p>{@code automaton} must be deterministic, as {@link ByteRunAutomaton} requires. {@code description} names
     * what the automaton was built from, for {@link #toString}; what the query is compared and cached on is the
     * automaton itself, so a caller that spells its parameters differently cannot make two queries collide.
     */
    public ColumnarStringAutomatonQuery(String field, Automaton automaton, String description, ScanBudget budget) {
        this(field, new ByteRunAutomaton(Objects.requireNonNull(automaton)), description, budget);
    }

    /**
     * As above, for a caller that already compiled the automaton - a fuzzy query, whose {@link ByteRunAutomaton} is built
     * by the query that defines the edit distance rather than by this one.
     */
    public ColumnarStringAutomatonQuery(String field, ByteRunAutomaton automaton, String description, ScanBudget budget) {
        this.field = Objects.requireNonNull(field);
        this.automaton = Objects.requireNonNull(automaton);
        this.description = Objects.requireNonNull(description);
        this.budget = Objects.requireNonNull(budget);
    }

    /**
     * A wildcard pattern, as the cheapest query that answers it.
     *
     * <p>{@code foo} is a term, {@code foo*} a prefix and {@code *foo*} a value carrying a run of bytes, and
     * a column answers each of those without an automaton. Anything else is one.
     */
    public static Query forWildcard(String field, String pattern, ScanBudget budget) {
        final String whole = literal(pattern);
        if (whole != null) {
            return ColumnarStringTermQuery.term(field, new BytesRef(whole), budget);
        }
        final String start = prefix(pattern);
        if (start != null) {
            return ColumnarStringTermQuery.prefix(field, new BytesRef(start), budget);
        }
        final String inside = contained(pattern);
        if (inside != null) {
            return ColumnarStringTermQuery.contains(field, new BytesRef(inside), budget);
        }
        return new ColumnarStringAutomatonQuery(
            field,
            Operations.determinize(
                WildcardQuery.toAutomaton(new Term(field, pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            ),
            "pattern=" + pattern,
            budget
        );
    }

    /**
     * The whole value a pattern names, or null where it names more than one.
     *
     * <p>The empty pattern is left to the automaton: Lucene reads it as naming no value at all rather than the
     * value of no bytes, and narrowing is only worth having while it answers exactly what the automaton would.
     */
    static String literal(String pattern) {
        return pattern.isEmpty() == false && plain(pattern) ? pattern : null;
    }

    /** The start a pattern names, as {@code foo*} does, or null where it names something else. */
    static String prefix(String pattern) {
        if (pattern.isEmpty() || pattern.charAt(pattern.length() - 1) != '*') {
            return null;
        }
        final String start = pattern.substring(0, pattern.length() - 1);
        return plain(start) ? start : null;
    }

    /** The run of bytes a pattern names, as {@code *foo*} does, or null where it names something else. */
    static String contained(String pattern) {
        if (pattern.length() < 3 || pattern.charAt(0) != '*' || pattern.charAt(pattern.length() - 1) != '*') {
            return null;
        }
        final String inside = pattern.substring(1, pattern.length() - 1);
        return plain(inside) ? inside : null;
    }

    /**
     * Whether a run of the pattern is bytes and nothing else. An escape is left to the automaton rather than
     * unescaped here, so that what the two agree on is what Lucene's own parser says a pattern means.
     */
    private static boolean plain(String pattern) {
        return pattern.indexOf('*') < 0 && pattern.indexOf('?') < 0 && pattern.indexOf('\\') < 0;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final LeafReader reader = context.reader();
                final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
                if (info == null || info.getDocValuesType() != DocValuesType.BINARY) {
                    // No value for the field in this segment, so nothing here matches.
                    return null;
                }
                return new ConstantScoreScorerSupplier(score(), scoreMode, reader.maxDoc()) {
                    @Override
                    public long cost() {
                        return reader.maxDoc();
                    }

                    @Override
                    public DocIdSetIterator iterator(long leadCost) throws IOException {
                        // Checked here rather than where the supplier is built: everything below allocates, and
                        // the answer is not wanted until Lucene asks for the iterator.
                        budget.check(searcher);
                        final BinaryDocValues values = reader.getBinaryDocValues(field);
                        if (values == null) {
                            return DocIdSetIterator.empty();
                        }
                        if (values instanceof StringColumnSource columnar) {
                            final StringColumnReader column = columnar.reader();
                            return column.match(value -> automaton.run(value.bytes, value.offset, value.length));
                        }

                        // An overlay rather than the column, as an updated field is: the values are read one
                        // document at a time and run through the automaton. The surface carries a document's slots
                        // as one payload, so each is run separately and any of them accepted accepts the document.
                        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
                        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(values) {
                            @Override
                            public boolean matches() throws IOException {
                                final int slots = decoder.reset(values.binaryValue());
                                for (int slot = 0; slot < slots; slot++) {
                                    final BytesRef candidate = decoder.next();
                                    // A null has no bytes to run an automaton over, not even the empty ones.
                                    if (candidate == null) {
                                        continue;
                                    }
                                    if (automaton.run(candidate.bytes, candidate.offset, candidate.length)) {
                                        return true;
                                    }
                                }
                                return false;
                            }

                            @Override
                            public float matchCost() {
                                return 100f;
                            }
                        });
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.consumeTermsMatching(this, field, () -> automaton);
        }
    }

    @Override
    public String toString(String defaultField) {
        return "ColumnarStringAutomatonQuery(field=" + field + "," + description + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ColumnarStringAutomatonQuery q) {
            return field.equals(q.field) && automaton.equals(q.automaton) && description.equals(q.description);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, automaton, description);
    }
}
