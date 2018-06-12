/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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

    /**
     * A sequence ID, which is made up of a sequence number (both the searchable
     * and doc_value version of the field) and the primary term.
     */
    public static class SequenceIDFields {

        public final Field seqNo;
        public final Field seqNoDocValue;
        public final Field primaryTerm;

        public SequenceIDFields(Field seqNo, Field seqNoDocValue, Field primaryTerm) {
            Objects.requireNonNull(seqNo, "sequence number field cannot be null");
            Objects.requireNonNull(seqNoDocValue, "sequence number dv field cannot be null");
            Objects.requireNonNull(primaryTerm, "primary term field cannot be null");
            this.seqNo = seqNo;
            this.seqNoDocValue = seqNoDocValue;
            this.primaryTerm = primaryTerm;
        }

        public static SequenceIDFields emptySeqID() {
            return new SequenceIDFields(new LongPoint(NAME, SequenceNumbers.UNASSIGNED_SEQ_NO),
                    new NumericDocValuesField(NAME, SequenceNumbers.UNASSIGNED_SEQ_NO),
                    new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
        }
    }

    public static final String NAME = "_seq_no";
    public static final String CONTENT_TYPE = "_seq_no";
    public static final String PRIMARY_TERM_NAME = "_primary_term";

    public static class SeqNoDefaults {
        public static final String NAME = SeqNoFieldMapper.NAME;
        public static final MappedFieldType FIELD_TYPE = new SeqNoFieldType();

        static {
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, SeqNoFieldMapper> {

        public Builder() {
            super(SeqNoDefaults.NAME, SeqNoDefaults.FIELD_TYPE, SeqNoDefaults.FIELD_TYPE);
        }

        @Override
        public SeqNoFieldMapper build(BuilderContext context) {
            return new SeqNoFieldMapper(context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new SeqNoFieldMapper(indexSettings);
        }
    }

    static final class SeqNoFieldType extends SimpleMappedFieldType {

        SeqNoFieldType() {
        }

        protected SeqNoFieldType(SeqNoFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new SeqNoFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private long parse(Object value) {
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
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            long v = parse(value);
            return LongPoint.newExactQuery(name(), v);
        }

        @Override
        public Query termsQuery(List<?> values, @Nullable QueryShardContext context) {
            long[] v = new long[values.size()];
            for (int i = 0; i < values.size(); ++i) {
                v[i] = parse(values.get(i));
            }
            return LongPoint.newSetQuery(name(), v);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower,
                                boolean includeUpper, QueryShardContext context) {
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
            return LongPoint.newRangeQuery(name(), l, u);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(NumericType.LONG);
        }

    }

    public SeqNoFieldMapper(Settings indexSettings) {
        super(NAME, SeqNoDefaults.FIELD_TYPE, SeqNoDefaults.FIELD_TYPE, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        // see InternalEngine.innerIndex to see where the real version value is set
        // also see ParsedDocument.updateSeqID (called by innerIndex)
        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        context.seqID(seqID);
        fields.add(seqID.seqNo);
        fields.add(seqID.seqNoDocValue);
        fields.add(seqID.primaryTerm);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // fields are added in parseCreateField
        return null;
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // In the case of nested docs, let's fill nested docs with the original
        // so that Lucene doesn't write a Bitset for documents that
        // don't have the field. This is consistent with the default value
        // for efficiency.
        // we share the parent docs fields to ensure good compression
        SequenceIDFields seqID = context.seqID();
        assert seqID != null;
        final Version versionCreated = context.mapperService().getIndexSettings().getIndexVersionCreated();
        final boolean includePrimaryTerm = versionCreated.before(Version.V_6_1_0);
        for (Document doc : context.nonRootDocuments()) {
            doc.add(seqID.seqNo);
            doc.add(seqID.seqNoDocValue);
            if (includePrimaryTerm) {
                // primary terms are used to distinguish between parent and nested docs since 6.1.0
                doc.add(seqID.primaryTerm);
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        // nothing to do
    }

}
