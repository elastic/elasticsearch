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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Mapper for the {@code _primary_term} field.
 *
 * The primary term is only used during collision after receiving identical seq#
 * values for two document copies. The primary term is stored as a doc value
 * field without being indexed, since it is only intended for use as a
 * key-value lookup.
 */
public class PrimaryTermFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_primary_term";
    public static final String CONTENT_TYPE = "_primary_term";

    public static class Defaults {

        public static final String NAME = PrimaryTermFieldMapper.NAME;
        public static final MappedFieldType FIELD_TYPE = new PrimaryTermFieldType();

        static {
            FIELD_TYPE.setName(NAME);
            // Need sorted doc_values for key-value lookup of primary term
            FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
            FIELD_TYPE.setHasDocValues(true);
            // We don't do any searches on this field, so don't even index it
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, PrimaryTermFieldMapper> {

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
        }

        @Override
        public PrimaryTermFieldMapper build(BuilderContext context) {
            return new PrimaryTermFieldMapper(context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new PrimaryTermFieldMapper(indexSettings);
        }
    }

    static final class PrimaryTermFieldType extends MappedFieldType {

        public PrimaryTermFieldType() {
        }

        protected PrimaryTermFieldType(PrimaryTermFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new PrimaryTermFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            throw new QueryShardException(context, "PrimaryTermField field [" + name() + "] is not searchable");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(NumericType.LONG);
        }

        @Override
        public FieldStats stats(IndexReader reader) throws IOException {
            // TODO: evaluate whether we need this at all, do we need field stats on this field?
            // nocommit remove implementation when late-binding commits are possible
            final List<LeafReaderContext> leaves = reader.leaves();
            if (leaves.isEmpty()) {
                return null;
            }

            long currentMin = Long.MAX_VALUE;
            long currentMax = Long.MIN_VALUE;
            boolean found = false;
            for (int i = 0; i < leaves.size(); i++) {
                final LeafReader leaf = leaves.get(i).reader();
                final NumericDocValues values = leaf.getNumericDocValues(name());
                if (values == null) continue;
                final Bits bits = leaf.getLiveDocs();
                for (int docID = 0; docID < leaf.maxDoc(); docID++) {
                    if (bits == null || bits.get(docID)) {
                        found = true;
                        final long value = values.get(docID);
                        currentMin = Math.min(currentMin, value);
                        currentMax = Math.max(currentMax, value);
                    }
                }
            }
            return found ? new FieldStats.Long(reader.maxDoc(), 0, -1, -1, false, false, currentMin, currentMax) : null;
        }

    }

    public PrimaryTermFieldMapper(Settings indexSettings) {
        super(NAME, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        // see IndexShard.prepareIndex* to see where the real version value is passed through
        final Field primaryTerm = new NumericDocValuesField(NAME, 0);
        context.primaryTerm(primaryTerm);
        fields.add(primaryTerm);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        return null;
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // In the case of nested docs, let's fill nested docs with primaryTerm=0
        // so that Lucene doesn't write a Bitset for documents that don't have
        // the field. This is consistent with the default value for efficiency.
        for (int i = 1; i < context.docs().size(); i++) {
            final Document doc = context.docs().get(i);
            doc.add(new NumericDocValuesField(NAME, 0L));
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
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // nothing to do
    }

}
