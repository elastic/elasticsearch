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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PercolatorFieldMapper extends MetadataFieldMapper {

    public static final String CONTENT_TYPE = "percolator";
    public static final PercolatorFieldType FIELD_TYPE = new PercolatorFieldType();
    public static final String FIELD_NAME = "query";

    public static class Builder extends MetadataFieldMapper.Builder<Builder, PercolatorFieldMapper> {

        private final QueryShardContext queryShardContext;

        public Builder(QueryShardContext queryShardContext) {
            super(FIELD_NAME, FIELD_TYPE);
            this.queryShardContext = queryShardContext;
        }

        @Override
        public PercolatorFieldMapper build(BuilderContext context) {
            return new PercolatorFieldMapper(name(), fieldType(), fieldType(), context.indexSettings(), queryShardContext);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private final IndexService indexService;

        public TypeParser(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(indexService.getQueryShardContext());
        }
    }

    public static final class PercolatorFieldType extends MappedFieldType {

        public PercolatorFieldType() {
            setNames(new Names(FIELD_NAME));
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

    private final QueryShardContext queryShardContext;

    public PercolatorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings, QueryShardContext queryShardContext) {
        super(simpleName, fieldType, defaultFieldType, indexSettings);
        this.queryShardContext = queryShardContext;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        String type = null;
        BytesReference querySource = null;
        try (XContentParser sourceParser = XContentHelper.createParser(context.source())) {
            String currentFieldName = null;
            XContentParser.Token token = sourceParser.nextToken(); // move the START_OBJECT
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchException("failed to parse query [" + context.id() + "], not starting with OBJECT");
            }
            while ((token = sourceParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = sourceParser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        if (type != null) {
                            Query query = parseQuery(type, sourceParser);
                            QueryMetadataService.extractQueryMetadata(query, context.doc());
                            return;
                        } else {
                            XContentBuilder builder = XContentFactory.contentBuilder(sourceParser.contentType());
                            builder.copyCurrentStructure(sourceParser);
                            querySource = builder.bytes();
                            builder.close();
                        }
                    } else {
                        sourceParser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    sourceParser.skipChildren();
                } else if (token.isValue()) {
                    if ("type".equals(currentFieldName)) {
                        type = sourceParser.text();
                    }
                }
            }
            try (XContentParser queryParser = XContentHelper.createParser(querySource)) {
                Query query = parseQuery(type, queryParser);
                QueryMetadataService.extractQueryMetadata(query, context.doc());
            }
        } catch (Exception e) {
            throw new PercolatorException(new Index(context.index()), "failed to parse query [" + context.id() + "]", e);
        }
    }

    private Query parseQuery(String type, XContentParser parser) throws IOException {
        String[] previousTypes = null;
        if (type != null) {
            previousTypes = QueryShardContext.setTypesWithPrevious(type);
        }
        QueryShardContext context = new QueryShardContext(queryShardContext);
        try {
            context.reset(parser);
            // This means that fields in the query need to exist in the mapping prior to registering this query
            // The reason that this is required, is that if a field doesn't exist then the query assumes defaults, which may be undesired.
            //
            // Even worse when fields mentioned in percolator queries do go added to map after the queries have been registered
            // then the percolator queries don't work as expected any more.
            //
            // Query parsing can't introduce new fields in mappings (which happens when registering a percolator query),
            // because field type can't be inferred from queries (like document do) so the best option here is to disallow
            // the usage of unmapped fields in percolator queries to avoid unexpected behaviour
            //
            // if index.percolator.map_unmapped_fields_as_string is set to true, query can contain unmapped fields which will be mapped
            // as an analyzed string.
            context.setAllowUnmappedFields(false);
            context.setMapUnmappedFieldAsString(false);
            return context.parseInnerQuery();
        } finally {
            if (type != null) {
                QueryShardContext.setTypes(previousTypes);
            }
            context.reset(null);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
