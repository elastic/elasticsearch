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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TermBasedFieldType;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class UidFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_uid";

    public static final String CONTENT_TYPE = "_uid";

    public static class Defaults {
        public static final String NAME = UidFieldMapper.NAME;

        public static final MappedFieldType FIELD_TYPE = new UidFieldType();
        public static final MappedFieldType NESTED_FIELD_TYPE;

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();

            NESTED_FIELD_TYPE = FIELD_TYPE.clone();
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new UidFieldMapper(indexSettings, fieldType);
        }
    }

    static final class UidFieldType extends TermBasedFieldType {

        public UidFieldType() {
        }

        protected UidFieldType(UidFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new UidFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            // TODO: add doc values support?
            return new PagedBytesIndexFieldData.Builder(
                    TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
                    TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
                    TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE);
        }
    }

    private UidFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing, Defaults.FIELD_TYPE, indexSettings);
    }

    private UidFieldMapper(MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings) {
        super(NAME, fieldType, defaultFieldType, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {}

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we do everything in preParse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        Field uid = new Field(NAME, Uid.createUid(context.sourceToParse().type(), context.sourceToParse().id()), Defaults.FIELD_TYPE);
        fields.add(uid);
        if (fieldType().hasDocValues()) {
            fields.add(new BinaryDocValuesField(NAME, new BytesRef(uid.stringValue())));
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
        // do nothing here, no merging, but also no exception
    }
}
