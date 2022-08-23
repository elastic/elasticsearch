/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.field.SeqNoDocValuesField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.Objects;

/**
 * Mapper for the {@code _seq_no} field.
 *
 * We expect to use the seq# for sorting, during collision checking and for
 * doing range searches. Therefore the {@code _seq_no} field is stored both
 * as a numeric doc value and as numeric indexed field.
 *
 * This mapper also manages the primary term field, which has no ES named
 * equivalent. The primary term is only used during collision after receiving
 * identical seq# values for two document copies. The primary term is stored as
 * a doc value field without being indexed, since it is only intended for use
 * as a key-value lookup.

 */
public class SeqNoFieldMapper extends MetadataFieldMapper {
    private static final Logger logger = LogManager.getLogger(SeqNoFieldMapper.class);

    /**
     * A sequence ID, which is made up of a sequence number (both the searchable
     * and doc_value version of the field) and the primary term.
     */
    public static class SequenceIDFields {

        private final boolean useShift;
        private final Field seqNoPoint;
        private final Field seqNoDocValue;
        private final Field primaryTerm;
        private final Field tombstoneField;

        private SequenceIDFields(Version indexVersionCreated, @Nullable Field tombstoneField) {
            this.useShift = indexVersionCreated.onOrAfter(Version.V_8_4_0);
            this.seqNoPoint = useShift
                ? new LongPoint(POINTS_NAME, SequenceNumbers.UNASSIGNED_SEQ_NO >> POINTS_SHIFT)
                : new LongPoint(NAME, SequenceNumbers.UNASSIGNED_SEQ_NO);
            this.seqNoDocValue = new NumericDocValuesField(NAME, SequenceNumbers.UNASSIGNED_SEQ_NO);
            this.primaryTerm = new NumericDocValuesField(PRIMARY_TERM_NAME, 0);
            this.tombstoneField = tombstoneField;
        }

        public void addFields(LuceneDocument document) {
            document.add(seqNoPoint);
            document.add(seqNoDocValue);
            document.add(primaryTerm);
            if (tombstoneField != null) {
                document.add(tombstoneField);
            }
        }

        /**
         * Update the values of the {@code _seq_no} and {@code primary_term} fields
         * to the specified value. Called in the engine long after parsing.
         */
        public void set(long seqNo, long primaryTerm) {
            // TODO negative values no shift?
            this.seqNoPoint.setLongValue(useShift ? seqNo >> POINTS_SHIFT : seqNo);
            this.seqNoDocValue.setLongValue(seqNo);
            this.primaryTerm.setLongValue(primaryTerm);
        }

        public static SequenceIDFields emptySeqID(Version indexVersionCreated) {
            return new SequenceIDFields(indexVersionCreated, null);
        }

        public static SequenceIDFields tombstone(Version indexVersionCreated) {
            return new SequenceIDFields(indexVersionCreated, new NumericDocValuesField(TOMBSTONE_NAME, 1));
        }
    }

    public static final String NAME = "_seq_no";
    public static final String CONTENT_TYPE = "_seq_no";
    public static final String PRIMARY_TERM_NAME = "_primary_term";
    public static final String TOMBSTONE_NAME = "_tombstone";
    public static final String POINTS_NAME = "_seq_no_points";
    /**
     * The number of bits of precision we throw away in the
     * {@link LongPoint points} for the {@code _seq_no}. If queries need
     * to make up for lost precision they double-check the {@code _seq_no}
     * from doc values.
     */
    private static final int POINTS_SHIFT = 10;
    /**
     * An estimate for the cost of rechecking the doc values for the
     * {@code _seq_no} of a single document.
     * See {@link TwoPhaseIterator#matchCost()} for units.
     */
    private static final float DV_RECHECK_COST = 1000f;

    public static final SeqNoFieldMapper INSTANCE = new SeqNoFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    public static final class SeqNoFieldType extends SimpleMappedFieldType {

        private static final SeqNoFieldType INSTANCE = new SeqNoFieldType();

        private SeqNoFieldType() {
            super(NAME, true, false, true, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private static long parse(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
                }
                if (doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }
                return ((Number) value).longValue();
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Long.parseLong(value.toString());
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return false;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return exactQuery(context.indexVersionCreated(), parse(value));
        }

        /**
         * Query for a precise sequence number.
         */
        public Query exactQuery(Version indexVersionCreated, long seqNo) {
            if (indexVersionCreated.before(Version.V_8_4_0)) {
                // Before 8.4.0 we wrote the points as _seq_no and didn't shift them
                return LongPoint.newExactQuery(NAME, seqNo);
            }
            // TODO negative values no shift?
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(LongPoint.newExactQuery(POINTS_NAME, seqNo >> POINTS_SHIFT), BooleanClause.Occur.MUST);
            builder.add(NumericDocValuesField.newSlowExactQuery(NAME, seqNo), BooleanClause.Occur.MUST);
            return builder.build();
        }

        @Override
        public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
            long[] v = values.stream().mapToLong(SeqNoFieldType::parse).toArray();
            if (context.indexVersionCreated().before(Version.V_8_4_0)) {
                // Before 8.4.0 we wrote the points as _seq_no and didn't shift them
                return LongPoint.newSetQuery(NAME, v);
            }
            return new SeqNoSetQuery(v);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                l = parse(lowerTerm);
                if (includeLower == false) {
                    if (l == Long.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    ++l;
                }
            }
            if (upperTerm != null) {
                u = parse(upperTerm);
                if (includeUpper == false) {
                    if (u == Long.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    --u;
                }
            }
            return rangeQuery(context.getIndexSettings().getIndexVersionCreated(), l, u);
        }

        /**
         * Query for a range of sequence numbers.
         */
        public Query rangeQuery(Version indexVersionCreated, long from, long to) {
            if (indexVersionCreated.before(Version.V_8_4_0)) {
                // Before 8.4.0 we wrote the points as _seq_no and didn't shift them
                return LongPoint.newRangeQuery(NAME, from, to);
            }
            // TODO hand rolled query that only scans on the lower and upper end and only if from and to aren't on the rounding boundary
            // TODO negative values no shift?

            long shiftedFrom = from >> POINTS_SHIFT;
            long unshiftedFrom = shiftedFrom << POINTS_SHIFT;
            long shiftedTo = to >> POINTS_SHIFT;
            long unshiftedTo = shiftedTo << POINTS_SHIFT;

            if (shiftedFrom == shiftedTo) {
                if (from == unshiftedFrom && to == unshiftedTo) {
                    logger.debug("pure point");
                    return LongPoint.newExactQuery(POINTS_NAME, shiftedFrom);
                }
                logger.debug("double check point");
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(LongPoint.newExactQuery(POINTS_NAME, shiftedFrom), BooleanClause.Occur.MUST);
                builder.add(NumericDocValuesField.newSlowRangeQuery(NAME, from, to), BooleanClause.Occur.MUST);
                return builder.build();
            }

            BooleanQuery.Builder disjunction = new BooleanQuery.Builder();
            disjunction.setMinimumNumberShouldMatch(1);
            if (from == unshiftedFrom) {
                logger.debug("low is pure point");
                disjunction.add(LongPoint.newExactQuery(POINTS_NAME, shiftedFrom), BooleanClause.Occur.SHOULD);
            } else {
                logger.debug("low is double check");
                BooleanQuery.Builder conjunction = new BooleanQuery.Builder();
                conjunction.add(LongPoint.newExactQuery(POINTS_NAME, shiftedFrom), BooleanClause.Occur.MUST);
                conjunction.add(NumericDocValuesField.newSlowRangeQuery(NAME, from, to), BooleanClause.Occur.MUST);
                disjunction.add(conjunction.build(), BooleanClause.Occur.SHOULD);
            }
            if (shiftedFrom + 1 != shiftedTo) {
                logger.debug("require middle");
                disjunction.add(LongPoint.newRangeQuery(POINTS_NAME, shiftedFrom + 1, shiftedTo - 1), BooleanClause.Occur.SHOULD);
            }
            if (to == Long.MAX_VALUE || to == unshiftedTo + (1 << POINTS_SHIFT) - 1) {
                logger.debug("high is pure point");
                disjunction.add(LongPoint.newExactQuery(POINTS_NAME, shiftedTo), BooleanClause.Occur.SHOULD);
            } else {
                logger.debug("high is double check");
                BooleanQuery.Builder conjunction = new BooleanQuery.Builder();
                conjunction.add(LongPoint.newExactQuery(POINTS_NAME, shiftedTo), BooleanClause.Occur.MUST);
                conjunction.add(NumericDocValuesField.newSlowRangeQuery(NAME, from, to), BooleanClause.Occur.MUST);
                disjunction.add(conjunction.build(), BooleanClause.Occur.SHOULD);
            }
            return disjunction.build();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(NAME, NumericType.LONG, SeqNoDocValuesField::new);
        }
    }

    private SeqNoFieldMapper() {
        super(SeqNoFieldType.INSTANCE);
        LogManager.getLogger(SeqNoFieldMapper.class).error("USING >> {}", POINTS_SHIFT);
    }

    @Override
    public void preParse(DocumentParserContext context) {
        // see InternalEngine.innerIndex to see where the real version value is set
        // also see ParsedDocument.updateSeqID (called by innerIndex)
        SequenceIDFields seqID = SequenceIDFields.emptySeqID(context.indexSettings().getIndexVersionCreated());
        context.seqID(seqID);
        seqID.addFields(context.doc());
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        // In the case of nested docs, let's fill nested docs with the original
        // so that Lucene doesn't write a Bitset for documents that
        // don't have the field. This is consistent with the default value
        // for efficiency.
        // we share the parent docs fields to ensure good compression
        SequenceIDFields seqID = context.seqID();
        assert seqID != null;
        for (LuceneDocument doc : context.nonRootDocuments()) {
            doc.add(seqID.seqNoPoint);
            doc.add(seqID.seqNoDocValue);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SeqNoFieldType fieldType() {
        return (SeqNoFieldType) mappedFieldType;
    }

    /**
     * Query against a set of {@code _seq_no}s. Mostly delegates to a
     * {@link LongPoint#newSetQuery set query} against the underlying
     * points but double checks the match with doc values.
     */
    private static class SeqNoSetQuery extends Query {
        private final long[] seqNos;
        private final Query shiftedQuery;

        SeqNoSetQuery(long[] seqNos) {
            this.seqNos = seqNos.clone();
            Arrays.sort(this.seqNos);
            Set<Long> shifted = new HashSet<>();
            for (long seqNo : this.seqNos) {
                shifted.add(seqNo >> POINTS_SHIFT);
            }
            shiftedQuery = LongPoint.newSetQuery(POINTS_NAME, shifted);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight shiftedWeight = shiftedQuery.createWeight(searcher, scoreMode, boost);
            return new ConstantScoreWeight(this, boost) {
                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    Scorer shiftedScorer = shiftedWeight.scorer(context);
                    NumericDocValues dv = context.reader().getNumericDocValues(NAME);
                    if (shiftedScorer.twoPhaseIterator() != null) {
                        throw new IllegalStateException("expected shifted query not to have two phase iterator");
                    }
                    return new ConstantScoreScorer(this, score(), scoreMode, new TwoPhaseIterator(shiftedScorer.iterator()) {
                        @Override
                        public boolean matches() throws IOException {
                            if (false == dv.advanceExact(approximation.docID())) {
                                return false;
                            }
                            long docSeqNo = dv.longValue();
                            int idx = Arrays.binarySearch(seqNos, docSeqNo);
                            return idx >= 0;
                        }

                        @Override
                        public float matchCost() {
                            return DV_RECHECK_COST;
                        }
                    });
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return shiftedWeight.isCacheable(ctx);
                }
            };
        }

        @Override
        public String toString(String field) {
            StringBuilder sb = new StringBuilder();
            if (POINTS_NAME.equals(field) == false) {
                sb.append(POINTS_NAME);
                sb.append(':');
            }

            sb.append("{");

            boolean first = true;
            for (long seqNo : seqNos) {
                if (first == false) {
                    sb.append(" ");
                }
                first = false;
                sb.append(seqNo);
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(POINTS_NAME)) {
                visitor.visitLeaf(this);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj)) {
                return false;
            }
            SeqNoSetQuery other = (SeqNoSetQuery) obj;
            return Arrays.equals(seqNos, other.seqNos);
        }

        @Override
        public int hashCode() {
            return classHash() * 31 + Arrays.hashCode(seqNos);
        }
    }
}
