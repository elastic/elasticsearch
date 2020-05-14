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
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/** Mapper for the doc_count field. */
public class DocCountFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "doc_count";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new DocCountFieldType();

        static {
            FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<DocCountFieldMapper.Builder, DocCountFieldMapper> {

        public Builder(String name) {
            super(name, DocCountFieldMapper.Defaults.FIELD_TYPE, DocCountFieldMapper.Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public DocCountFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new DocCountFieldMapper(name, fieldType, defaultFieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public DocCountFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            DocCountFieldMapper.Builder builder = new DocCountFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    static final class DocCountFieldType extends MappedFieldType {

        DocCountFieldType() {
        }

        protected DocCountFieldType(DocCountFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new DocCountFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Field [" + name() + " ]of type [" + CONTENT_TYPE + "] is not searchable");

        }
    }

    protected DocCountFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, MultiFields.empty(), CopyTo.empty());

    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        Number value;
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NUMBER) {
            value = context.parser().numberValue().floatValue();
        } else {
            return;
        }

        if (value != null) {
            if (value.longValue() < 0 || value.floatValue() != value.longValue()) {
                throw new IllegalArgumentException(
                    "Field [" + fieldType.name() + "] must always be a positive integer");
            }

            final Field docCount = new NumericDocValuesField(name(), value.longValue());
            context.doc().add(docCount);
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
