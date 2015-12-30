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
package org.elasticsearch.index.percolator;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PercolatorFieldMapper extends FieldMapper {

    public static final String NAME = "query";
    public static final String CONTENT_TYPE = "percolator";
    public static final PercolatorFieldType FIELD_TYPE = new PercolatorFieldType();

    public static class Builder extends FieldMapper.Builder<Builder, PercolatorFieldMapper> {

        private final QueryShardContext queryShardContext;

        public Builder(QueryShardContext queryShardContext) {
            super(NAME, FIELD_TYPE, FIELD_TYPE);
            this.queryShardContext = queryShardContext;
        }

        @Override
        public PercolatorFieldMapper build(BuilderContext context) {
            return new PercolatorFieldMapper(name(), fieldType(), fieldType(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo, queryShardContext);
        }
    }

    public static class TypeParser implements FieldMapper.TypeParser {

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(parserContext.queryShardContext());
        }
    }

    public static final class PercolatorFieldType extends MappedFieldType {

        public PercolatorFieldType() {
            setName(NAME);
            setIndexOptions(IndexOptions.NONE);
            setDocValuesType(DocValuesType.NONE);
            setStored(false);
        }

        public PercolatorFieldType(MappedFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new PercolatorFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    private final boolean mapUnmappedFieldAsString;
    private final QueryShardContext queryShardContext;

    public PercolatorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings, MultiFields multiFields, CopyTo copyTo, QueryShardContext queryShardContext) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.queryShardContext = queryShardContext;
        this.mapUnmappedFieldAsString = indexSettings.getAsBoolean(PercolatorQueriesRegistry.MAP_UNMAPPED_FIELDS_AS_STRING, false);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        QueryShardContext queryShardContext = new QueryShardContext(this.queryShardContext);
        Query query = PercolatorQueriesRegistry.parseQuery(queryShardContext, mapUnmappedFieldAsString, context.parser());
        if (context.flyweight() == false) {
            QueryMetadataService.extractQueryMetadata(query, context.doc());
        }
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("should not be invoked");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
