/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.tree.Source;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.util.SourceUtils.readSource;
import static org.elasticsearch.xpack.ql.util.SourceUtils.writeSource;

/**
 * Lucene query that wraps another query and only selects documents that match
 * the wrapped query <strong>and</strong> have a single field value.
 * <p>
 *     This allows us to wrap regular lucene queries to have ESQL style semantics
 *     which will allow us to continue to push expressions to Lucene.
 * </p>
 * <p>
 *     We could have chosen not to wrap the lucene query and instead double check
 *     the results after they are loaded. That could be faster in some cases, but
 *     for now we're going to always wrap so we can always push. When we find cases
 *     where double checking is better we'll try that.
 * </p>
 */
public class SingleValueQuery extends Query {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "esql_single_value",
        Builder::new
    );

    public static final String MULTI_VALUE_WARNING = "single-value function encountered multi-value";

    private final Query next;
    private final String field;

    public SingleValueQuery(Query next, String field) {
        super(next.source());
        this.next = next;
        this.field = field;
    }

    @Override
    public boolean containsNestedField(String path, String field) {
        return next.containsNestedField(path, field);
    }

    @Override
    public Query addNestedField(String path, String field, String format, boolean hasDocValues) {
        return next.addNestedField(path, field, format, hasDocValues);
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {
        next.enrichNestedSort(sort);
    }

    @Override
    public Builder asBuilder() {
        return new Builder(next.asBuilder(), field, new Stats(), next.source());
    }

    @Override
    protected String innerToString() {
        return next.toString();
    }

    @Override
    public SingleValueQuery negate(Source source) {
        return new SingleValueQuery(next.negate(source), field);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass() || false == super.equals(o)) {
            return false;
        }
        SingleValueQuery other = (SingleValueQuery) o;
        return Objects.equals(next, other.next) && Objects.equals(field, other.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), next, field);
    }

    public static class Builder extends AbstractQueryBuilder<Builder> {
        private final QueryBuilder next;
        private final String field;
        private final Stats stats;
        private final Source source;

        Builder(QueryBuilder next, String field, Stats stats, Source source) {
            this.next = next;
            this.field = field;
            this.stats = stats;
            this.source = source;
        }

        Builder(StreamInput in) throws IOException {
            super(in);
            this.next = in.readNamedWriteable(QueryBuilder.class);
            this.field = in.readString();
            this.stats = new Stats();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                this.source = readSource(in);
            } else {
                this.source = Source.EMPTY;

            }
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(next);
            out.writeString(field);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                writeSource(out, source);
            }
        }

        public QueryBuilder next() {
            return next;
        }

        public String field() {
            return field;
        }

        public Source source() {
            return source;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(ENTRY.name);
            builder.field("field", field);
            builder.field("next", next, params);
            builder.field("source", source.toString());
            builder.endObject();
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X; // the first version of ESQL
        }

        @Override
        protected org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) throws IOException {
            MappedFieldType ft = context.getFieldType(field);
            if (ft == null) {
                stats.missingField++;
                return new MatchNoDocsQuery("missing field [" + field + "]");
            }
            return new LuceneQuery(
                next.toQuery(context),
                context.getForField(ft, MappedFieldType.FielddataOperation.SEARCH),
                stats,
                new Warnings(source)
            );
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            QueryBuilder rewritten = next.rewrite(queryRewriteContext);
            if (rewritten instanceof MatchNoneQueryBuilder) {
                stats.rewrittenToMatchNone++;
                return rewritten;
            }
            if (rewritten == next) {
                return this;
            }
            return new Builder(rewritten, field, stats, source);
        }

        @Override
        protected boolean doEquals(Builder other) {
            return next.equals(other.next) && field.equals(other.field);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(next, field);
        }

        Stats stats() {
            return stats;
        }
    }

    private static class LuceneQuery extends org.apache.lucene.search.Query {
        final org.apache.lucene.search.Query next;
        private final IndexFieldData<?> fieldData;
        private final Stats stats;
        private final Warnings warnings;

        LuceneQuery(org.apache.lucene.search.Query next, IndexFieldData<?> fieldData, Stats stats, Warnings warnings) {
            this.next = next;
            this.fieldData = fieldData;
            this.stats = stats;
            this.warnings = warnings;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(fieldData.getFieldName())) {
                visitor.visitLeaf(next);
            }
        }

        @Override
        public org.apache.lucene.search.Query rewrite(IndexReader reader) throws IOException {
            org.apache.lucene.search.Query rewritten = next.rewrite(reader);
            if (rewritten instanceof MatchNoDocsQuery) {
                stats.rewrittenToMatchNone++;
                return rewritten;
            }
            if (rewritten == next) {
                return this;
            }
            return new LuceneQuery(rewritten, fieldData, stats, warnings);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new SingleValueWeight(this, next.createWeight(searcher, scoreMode, boost), fieldData, warnings);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            SingleValueQuery.LuceneQuery other = (SingleValueQuery.LuceneQuery) obj;
            return next.equals(other.next)
                && fieldData.getFieldName().equals(other.fieldData.getFieldName())
                && warnings.equals(other.warnings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), next, fieldData, warnings);
        }

        @Override
        public String toString(String field) {
            StringBuilder builder = new StringBuilder("single_value(");
            if (false == this.fieldData.getFieldName().equals(field)) {
                builder.append(this.fieldData.getFieldName());
                builder.append(":");
            }
            builder.append(next);
            return builder.append(")").toString();
        }
    }

    private static class SingleValueWeight extends Weight {
        private final Stats stats;
        private final Weight next;
        private final IndexFieldData<?> fieldData;
        private final Warnings warnings;

        private SingleValueWeight(SingleValueQuery.LuceneQuery query, Weight next, IndexFieldData<?> fieldData, Warnings warnings) {
            super(query);
            this.stats = query.stats;
            this.next = next;
            this.fieldData = fieldData;
            this.warnings = warnings;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation nextExplanation = next.explain(context, doc);
            if (false == nextExplanation.isMatch()) {
                return Explanation.noMatch("next didn't match", nextExplanation);
            }
            LeafFieldData lfd = fieldData.load(context);
            SortedBinaryDocValues values = lfd.getBytesValues();
            if (false == values.advanceExact(doc)) {
                return Explanation.noMatch("no values in field", nextExplanation);
            }
            if (values.docValueCount() != 1) {
                return Explanation.noMatch("field has too many values [" + values.docValueCount() + "]", nextExplanation);
            }
            return Explanation.match(nextExplanation.getValue(), "field has exactly 1 value", nextExplanation);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer nextScorer = next.scorer(context);
            if (nextScorer == null) {
                stats.noNextScorer++;
                return null;
            }
            LeafFieldData lfd = fieldData.load(context);
            /*
             * SortedBinaryDocValues are available for most fields, but they
             * are made available by eagerly converting non-bytes values to
             * utf-8 strings. The eager conversion is quite expensive. So
             * we specialize on numeric fields and fields with ordinals to
             * avoid that expense in at least that case.
             *
             * Also! Lucene's FieldExistsQuery only needs one scorer that can
             * use all the docs values iterators at DocIdSetIterators. We
             * can't do that because we need the check the number of fields.
             */
            if (lfd instanceof LeafNumericFieldData n) {
                return scorer(context, nextScorer, n);
            }
            if (lfd instanceof LeafOrdinalsFieldData o) {
                return scorer(context, nextScorer, o);
            }
            return scorer(nextScorer, lfd);
        }

        private Scorer scorer(LeafReaderContext context, Scorer nextScorer, LeafNumericFieldData lfd) throws IOException {
            SortedNumericDocValues sortedNumerics = lfd.getLongValues();
            if (DocValues.unwrapSingleton(sortedNumerics) != null) {
                /*
                 * Segment contains only single valued fields. But it's possible
                 * that some fields have 0 values. The most surefire way to check
                 * is to look at the index for the data. If there isn't an index
                 * this isn't going to work - but if there is we can compare the
                 * number of documents in the index to the number of values in it -
                 * if they are the same we've got a dense singleton.
                 */
                PointValues points = context.reader().getPointValues(fieldData.getFieldName());
                if (points != null && points.getDocCount() == context.reader().maxDoc()) {
                    stats.numericSingle++;
                    return nextScorer;
                }
            }
            TwoPhaseIterator nextIterator = nextScorer.twoPhaseIterator();
            if (nextIterator == null) {
                stats.numericMultiNoApprox++;
                return new SingleValueQueryScorer(
                    this,
                    nextScorer,
                    new TwoPhaseIteratorForSortedNumericsAndSinglePhaseQueries(nextScorer.iterator(), sortedNumerics, warnings)
                );
            }
            stats.numericMultiApprox++;
            return new SingleValueQueryScorer(
                this,
                nextScorer,
                new TwoPhaseIteratorForSortedNumericsAndTwoPhaseQueries(nextIterator, sortedNumerics, warnings)
            );
        }

        private Scorer scorer(LeafReaderContext context, Scorer nextScorer, LeafOrdinalsFieldData lfd) throws IOException {
            SortedSetDocValues sortedSet = lfd.getOrdinalsValues();
            if (DocValues.unwrapSingleton(sortedSet) != null) {
                /*
                 * Segment contains only single valued fields. But it's possible
                 * that some fields have 0 values. The most surefire way to check
                 * is to look at the index for the data. If there isn't an index
                 * this isn't going to work - but if there is we can compare the
                 * number of documents in the index to the number of values in it -
                 * if they are the same we've got a dense singleton.
                 */
                Terms terms = context.reader().terms(fieldData.getFieldName());
                if (terms != null && terms.getDocCount() == context.reader().maxDoc()) {
                    stats.ordinalsSingle++;
                    return nextScorer;
                }
            }
            TwoPhaseIterator nextIterator = nextScorer.twoPhaseIterator();
            if (nextIterator == null) {
                stats.ordinalsMultiNoApprox++;
                return new SingleValueQueryScorer(
                    this,
                    nextScorer,
                    new TwoPhaseIteratorForSortedSetAndSinglePhaseQueries(nextScorer.iterator(), sortedSet, warnings)
                );
            }
            stats.ordinalsMultiApprox++;
            return new SingleValueQueryScorer(
                this,
                nextScorer,
                new TwoPhaseIteratorForSortedSetAndTwoPhaseQueries(nextIterator, sortedSet, warnings)
            );
        }

        private Scorer scorer(Scorer nextScorer, LeafFieldData lfd) {
            SortedBinaryDocValues sortedBinary = lfd.getBytesValues();
            TwoPhaseIterator nextIterator = nextScorer.twoPhaseIterator();
            if (nextIterator == null) {
                stats.bytesNoApprox++;
                return new SingleValueQueryScorer(
                    this,
                    nextScorer,
                    new TwoPhaseIteratorForSortedBinaryAndSinglePhaseQueries(nextScorer.iterator(), sortedBinary, warnings)
                );
            }
            stats.bytesApprox++;
            return new SingleValueQueryScorer(
                this,
                nextScorer,
                new TwoPhaseIteratorForSortedBinaryAndTwoPhaseQueries(nextIterator, sortedBinary, warnings)
            );
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return next.isCacheable(ctx);
        }
    }

    private static class SingleValueQueryScorer extends Scorer {
        private final Scorer next;
        private final TwoPhaseIterator iterator;

        private SingleValueQueryScorer(Weight weight, Scorer next, TwoPhaseIterator iterator) {
            super(weight);
            this.next = next;
            this.iterator = iterator;
        }

        @Override
        public DocIdSetIterator iterator() {
            return TwoPhaseIterator.asDocIdSetIterator(iterator);
        }

        @Override
        public TwoPhaseIterator twoPhaseIterator() {
            return iterator;
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return next.getMaxScore(upTo);
        }

        @Override
        public float score() throws IOException {
            return next.score();
        }

        @Override
        public int docID() {
            return next.docID();
        }
    }

    /**
     * The estimated number of comparisons to check if a {@link SortedNumericDocValues}
     * has more than one value. There isn't a good way to get that number out of
     * {@link SortedNumericDocValues} so this is a guess.
     */
    private static final int SORTED_NUMERIC_MATCH_COST = 10;

    private static class TwoPhaseIteratorForSortedNumericsAndSinglePhaseQueries extends TwoPhaseIterator {
        private final SortedNumericDocValues sortedNumerics;
        private final Warnings warnings;

        private TwoPhaseIteratorForSortedNumericsAndSinglePhaseQueries(
            DocIdSetIterator approximation,
            SortedNumericDocValues sortedNumerics,
            Warnings warning
        ) {
            super(approximation);
            this.sortedNumerics = sortedNumerics;
            this.warnings = warning;
        }

        @Override
        public boolean matches() throws IOException {
            if (false == sortedNumerics.advanceExact(approximation.docID())) {
                return false;
            }
            if (sortedNumerics.docValueCount() != 1) {
                warnings.registerException(new IllegalArgumentException(MULTI_VALUE_WARNING));
                return false;
            }
            return true;
        }

        @Override
        public float matchCost() {
            return SORTED_NUMERIC_MATCH_COST;
        }
    }

    private static class TwoPhaseIteratorForSortedNumericsAndTwoPhaseQueries extends TwoPhaseIterator {
        private final SortedNumericDocValues sortedNumerics;
        private final TwoPhaseIterator next;
        private final Warnings warnings;

        private TwoPhaseIteratorForSortedNumericsAndTwoPhaseQueries(
            TwoPhaseIterator next,
            SortedNumericDocValues sortedNumerics,
            Warnings warnings
        ) {
            super(next.approximation());
            this.sortedNumerics = sortedNumerics;
            this.next = next;
            this.warnings = warnings;
        }

        @Override
        public boolean matches() throws IOException {
            if (false == sortedNumerics.advanceExact(approximation.docID())) {
                return false;
            }
            if (sortedNumerics.docValueCount() != 1) {
                warnings.registerException(new IllegalArgumentException(MULTI_VALUE_WARNING));
                return false;
            }
            return next.matches();
        }

        @Override
        public float matchCost() {
            return SORTED_NUMERIC_MATCH_COST + next.matchCost();
        }
    }

    private static class TwoPhaseIteratorForSortedBinaryAndSinglePhaseQueries extends TwoPhaseIterator {
        private final SortedBinaryDocValues sortedBinary;
        private final Warnings warnings;

        private TwoPhaseIteratorForSortedBinaryAndSinglePhaseQueries(
            DocIdSetIterator approximation,
            SortedBinaryDocValues sortedBinary,
            Warnings warnings
        ) {
            super(approximation);
            this.sortedBinary = sortedBinary;
            this.warnings = warnings;
        }

        @Override
        public boolean matches() throws IOException {
            if (false == sortedBinary.advanceExact(approximation.docID())) {
                return false;
            }
            if (sortedBinary.docValueCount() != 1) {
                warnings.registerException(new IllegalArgumentException(MULTI_VALUE_WARNING));
                return false;
            }
            return true;
        }

        @Override
        public float matchCost() {
            return SORTED_NUMERIC_MATCH_COST;
        }
    }

    private static class TwoPhaseIteratorForSortedSetAndTwoPhaseQueries extends TwoPhaseIterator {
        private final SortedSetDocValues sortedSet;
        private final TwoPhaseIterator next;
        private final Warnings warnings;

        private TwoPhaseIteratorForSortedSetAndTwoPhaseQueries(TwoPhaseIterator next, SortedSetDocValues sortedSet, Warnings warnings) {
            super(next.approximation());
            this.sortedSet = sortedSet;
            this.next = next;
            this.warnings = warnings;
        }

        @Override
        public boolean matches() throws IOException {
            if (false == sortedSet.advanceExact(approximation.docID())) {
                return false;
            }
            if (sortedSet.docValueCount() != 1) {
                warnings.registerException(new IllegalArgumentException(MULTI_VALUE_WARNING));
                return false;
            }
            return next.matches();
        }

        @Override
        public float matchCost() {
            return SORTED_NUMERIC_MATCH_COST + next.matchCost();
        }
    }

    private static class TwoPhaseIteratorForSortedSetAndSinglePhaseQueries extends TwoPhaseIterator {
        private final SortedSetDocValues sortedSet;
        private final Warnings warnings;

        private TwoPhaseIteratorForSortedSetAndSinglePhaseQueries(
            DocIdSetIterator approximation,
            SortedSetDocValues sortedSet,
            Warnings warnings
        ) {
            super(approximation);
            this.sortedSet = sortedSet;
            this.warnings = warnings;
        }

        @Override
        public boolean matches() throws IOException {
            if (false == sortedSet.advanceExact(approximation.docID())) {
                return false;
            }
            if (sortedSet.docValueCount() != 1) {
                warnings.registerException(new IllegalArgumentException(MULTI_VALUE_WARNING));
                return false;
            }
            return true;
        }

        @Override
        public float matchCost() {
            return SORTED_NUMERIC_MATCH_COST;
        }
    }

    private static class TwoPhaseIteratorForSortedBinaryAndTwoPhaseQueries extends TwoPhaseIterator {
        private final SortedBinaryDocValues sortedBinary;
        private final TwoPhaseIterator next;
        private final Warnings warnings;

        private TwoPhaseIteratorForSortedBinaryAndTwoPhaseQueries(
            TwoPhaseIterator next,
            SortedBinaryDocValues sortedBinary,
            Warnings warnings
        ) {
            super(next.approximation());
            this.sortedBinary = sortedBinary;
            this.next = next;
            this.warnings = warnings;
        }

        @Override
        public boolean matches() throws IOException {
            if (false == sortedBinary.advanceExact(approximation.docID())) {
                return false;
            }
            if (sortedBinary.docValueCount() != 1) {
                warnings.registerException(new IllegalArgumentException(MULTI_VALUE_WARNING));
                return false;
            }
            return next.matches();
        }

        @Override
        public float matchCost() {
            return SORTED_NUMERIC_MATCH_COST + next.matchCost();
        }
    }

    static class Stats {
        // TODO expose stats somehow
        private int missingField;
        private int rewrittenToMatchNone;
        private int noNextScorer;
        private int numericSingle;
        private int numericMultiNoApprox;
        private int numericMultiApprox;
        private int ordinalsSingle;
        private int ordinalsMultiNoApprox;
        private int ordinalsMultiApprox;
        private int bytesNoApprox;
        private int bytesApprox;

        int missingField() {
            return missingField;
        }

        int rewrittenToMatchNone() {
            return rewrittenToMatchNone;
        }

        int noNextScorer() {
            return noNextScorer;
        }

        int numericSingle() {
            return numericSingle;
        }

        int numericMultiNoApprox() {
            return numericMultiNoApprox;
        }

        int numericMultiApprox() {
            return numericMultiApprox;
        }

        int ordinalsSingle() {
            return ordinalsSingle;
        }

        int ordinalsMultiNoApprox() {
            return ordinalsMultiNoApprox;
        }

        int ordinalsMultiApprox() {
            return ordinalsMultiApprox;
        }

        int bytesNoApprox() {
            return bytesNoApprox;
        }

        int bytesApprox() {
            return bytesApprox;
        }
    }
}
