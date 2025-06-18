/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.script.field.SeqNoDocValuesField;

import java.util.Collection;
import java.util.Collections;

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

    // Like Lucene's LongField but single-valued (NUMERIC doc values instead of SORTED_NUMERIC doc values)
    private static class SingleValueLongField extends Field {

        private static final FieldType FIELD_TYPE;
        static {
            FieldType ft = new FieldType();
            ft.setDimensions(1, Long.BYTES);
            ft.setDocValuesType(DocValuesType.NUMERIC);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }

        SingleValueLongField(String field) {
            super(field, FIELD_TYPE);
            fieldsData = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }

        @Override
        public BytesRef binaryValue() {
            final byte[] pointValue = new byte[Long.BYTES];
            NumericUtils.longToSortableBytes((Long) fieldsData, pointValue, 0);
            return new BytesRef(pointValue);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
        }

    }

    /**
     * A sequence ID, which is made up of a sequence number (both the searchable
     * and doc_value version of the field) and the primary term.
     */
    public static class SequenceIDFields {

        private static final Field TOMBSTONE_FIELD = new NumericDocValuesField(TOMBSTONE_NAME, 1);

        private final Field seqNo;
        private final Field primaryTerm = new NumericDocValuesField(PRIMARY_TERM_NAME, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        private final boolean isTombstone;

        private SequenceIDFields(SeqNoIndexOptions seqNoIndexOptions, boolean isTombstone) {
            this.isTombstone = isTombstone;
            this.seqNo = switch (seqNoIndexOptions) {
                case POINTS_AND_DOC_VALUES -> new SingleValueLongField(NAME);
                case DOC_VALUES_ONLY -> NumericDocValuesField.indexedField(NAME, SequenceNumbers.UNASSIGNED_SEQ_NO);
            };
        }

        public void addFields(LuceneDocument document) {
            document.add(seqNo);
            document.add(primaryTerm);
            if (isTombstone) {
                document.add(TOMBSTONE_FIELD);
            }
        }

        /**
         * Update the values of the {@code _seq_no} and {@code primary_term} fields
         * to the specified value. Called in the engine long after parsing.
         */
        public void set(long seqNo, long primaryTerm) {
            this.seqNo.setLongValue(seqNo);
            this.primaryTerm.setLongValue(primaryTerm);
        }

        /**
         * Build and empty sequence ID who's values can be assigned later by
         * calling {@link #set}.
         */
        public static SequenceIDFields emptySeqID(SeqNoIndexOptions seqNoIndexOptions) {
            return new SequenceIDFields(seqNoIndexOptions, false);
        }

        public static SequenceIDFields tombstone(SeqNoIndexOptions seqNoIndexOptions) {
            return new SequenceIDFields(seqNoIndexOptions, true);
        }
    }

    public static final String NAME = "_seq_no";
    public static final String CONTENT_TYPE = "_seq_no";
    public static final String PRIMARY_TERM_NAME = "_primary_term";
    public static final String TOMBSTONE_NAME = "_tombstone";

    public static final SeqNoFieldMapper WITH_POINT = new SeqNoFieldMapper(true);
    public static final SeqNoFieldMapper NO_POINT = new SeqNoFieldMapper(false);

    public static final TypeParser PARSER = new FixedTypeParser(c -> switch (c.getIndexSettings().seqNoIndexOptions()) {
        case POINTS_AND_DOC_VALUES -> WITH_POINT;
        case DOC_VALUES_ONLY -> NO_POINT;
    });

    static final class SeqNoFieldType extends SimpleMappedFieldType {
        private static final SeqNoFieldType WITH_POINT = new SeqNoFieldType(true);
        private static final SeqNoFieldType NO_POINT = new SeqNoFieldType(false);

        private SeqNoFieldType(boolean indexed) {
            super(NAME, indexed, false, true, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, Collections.emptyMap());
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
            throw new IllegalArgumentException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
            long v = parse(value);
            if (isIndexed()) {
                return LongPoint.newExactQuery(name(), v);
            } else {
                return NumericDocValuesField.newSlowExactQuery(name(), v);
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
            long[] v = values.stream().mapToLong(SeqNoFieldType::parse).toArray();
            if (isIndexed()) {
                return LongPoint.newSetQuery(name(), v);
            } else {
                return NumericDocValuesField.newSlowSetQuery(name(), v);
            }
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
            return rangeQueryForSeqNo(isIndexed(), l, u);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.LONG, SeqNoDocValuesField::new, isIndexed());
        }

        @Override
        public boolean isSearchable() {
            return isIndexed() || hasDocValues();
        }
    }

    private SeqNoFieldMapper(boolean indexedPoints) {
        super(indexedPoints ? SeqNoFieldType.WITH_POINT : SeqNoFieldType.NO_POINT);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        // see InternalEngine.innerIndex to see where the real version value is set
        // also see ParsedDocument.updateSeqID (called by innerIndex)
        // In the case of nested docs, let's fill nested docs with the original
        // so that Lucene doesn't write a Bitset for documents that
        // don't have the field. This is consistent with the default value
        // for efficiency.
        // we share the parent docs fields to ensure good compression
        SequenceIDFields seqID = context.seqID();
        seqID.addFields(context.doc());
        for (LuceneDocument doc : context.nonRootDocuments()) {
            doc.add(seqID.seqNo);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private static Query rangeQueryForSeqNo(boolean withPoints, long lowerValue, long upperValue) {
        if (withPoints) {
            // TODO: Use IndexOrDocValuesQuery
            return LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, lowerValue, upperValue);
        } else {
            return NumericDocValuesField.newSlowRangeQuery(SeqNoFieldMapper.NAME, lowerValue, upperValue);
        }
    }

    /**
     * Create a range query that matches all documents whose seq_no is between {@code lowerValue} and {@code upperValue} included.
     */
    public static Query rangeQueryForSeqNo(SeqNoIndexOptions seqNoIndexOptions, long lowerValue, long upperValue) {
        return switch (seqNoIndexOptions) {
            case POINTS_AND_DOC_VALUES -> rangeQueryForSeqNo(true, lowerValue, upperValue);
            case DOC_VALUES_ONLY -> rangeQueryForSeqNo(false, lowerValue, upperValue);
        };
    }

    public enum SeqNoIndexOptions {
        DOC_VALUES_ONLY,
        POINTS_AND_DOC_VALUES,
    }
}
