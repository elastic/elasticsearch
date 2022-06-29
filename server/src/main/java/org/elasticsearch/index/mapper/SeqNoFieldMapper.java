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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.script.field.SeqNoDocValuesField;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Supplier;

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

    /**
     * A sequence ID, which is made up of a sequence number (both the searchable
     * and doc_value version of the field) and the primary term.
     */
    public static class SequenceIDFields {

        private final Field seqNoPoint;
        private final Field seqNoDocValue;
        private final Field primaryTerm;
        private final Field tombstoneField;

        private SequenceIDFields(Field seqNoPoint, Field seqNoDocValue, Field primaryTerm, @Nullable Field tombstoneField) {
            Objects.requireNonNull(seqNoPoint, "sequence number field cannot be null");
            Objects.requireNonNull(seqNoDocValue, "sequence number dv field cannot be null");
            Objects.requireNonNull(primaryTerm, "primary term field cannot be null");
            this.seqNoPoint = seqNoPoint;
            this.seqNoDocValue = seqNoDocValue;
            this.primaryTerm = primaryTerm;
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
            this.seqNoPoint.setLongValue(seqNo >> POINTS_SHIFT);
            this.seqNoDocValue.setLongValue(seqNo);
            this.primaryTerm.setLongValue(primaryTerm);
        }

        public static SequenceIDFields emptySeqID() {
            return new SequenceIDFields(
                new LongPoint(POINTS_NAME, SequenceNumbers.UNASSIGNED_SEQ_NO >> POINTS_SHIFT),
                new NumericDocValuesField(NAME, SequenceNumbers.UNASSIGNED_SEQ_NO),
                new NumericDocValuesField(PRIMARY_TERM_NAME, 0),
                null
            );
        }

        public static SequenceIDFields tombstone() {
            return new SequenceIDFields(
                new LongPoint(POINTS_NAME, SequenceNumbers.UNASSIGNED_SEQ_NO >> POINTS_SHIFT),
                new NumericDocValuesField(NAME, SequenceNumbers.UNASSIGNED_SEQ_NO),
                new NumericDocValuesField(PRIMARY_TERM_NAME, 0),
                new NumericDocValuesField(TOMBSTONE_NAME, 1)
            );
        }
    }

    public static final String NAME = "_seq_no";
    public static final String CONTENT_TYPE = "_seq_no";
    public static final String PRIMARY_TERM_NAME = "_primary_term";
    public static final String TOMBSTONE_NAME = "_tombstone";
    public static final String POINTS_NAME = "_seq_no_points";
    private static final int POINTS_SHIFT = 8;

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
        public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
            long v = parse(value);
            // TODO recheck the doc values
            throw new UnsupportedOperationException();
            // return LongPoint.newExactQuery(name(), v);
        }

        public Query exactQuery(long seqNo) {
            // TODO hand rolled query?
            // TODO negative values no shift?
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(LongPoint.newExactQuery(POINTS_NAME, seqNo >> POINTS_SHIFT), BooleanClause.Occur.MUST);
            builder.add(NumericDocValuesField.newSlowExactQuery(NAME, seqNo), BooleanClause.Occur.MUST);
            return builder.build();
        }

        @Override
        public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
            long[] v = values.stream().mapToLong(SeqNoFieldType::parse).toArray();
            // TODO recheck the doc values
            throw new UnsupportedOperationException();
            // return LongPoint.newSetQuery(name(), v);
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
            return rangeQuery(l, u);
        }

        public Query rangeQuery(long from, long to) {
            // TODO hand rolled query that only scans on the lower and upper end and only if from and to aren't on the rounding boundary
            // TODO negative values no shift?
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(LongPoint.newRangeQuery(POINTS_NAME, from >> POINTS_SHIFT, to >> POINTS_SHIFT), BooleanClause.Occur.MUST);
            builder.add(NumericDocValuesField.newSlowRangeQuery(NAME, from, to), BooleanClause.Occur.MUST);
            return builder.build();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(NAME, NumericType.LONG, SeqNoDocValuesField::new);
        }
    }

    private SeqNoFieldMapper() {
        super(SeqNoFieldType.INSTANCE);
    }

    @Override
    public void preParse(DocumentParserContext context) {
        // see InternalEngine.innerIndex to see where the real version value is set
        // also see ParsedDocument.updateSeqID (called by innerIndex)
        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
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
}
