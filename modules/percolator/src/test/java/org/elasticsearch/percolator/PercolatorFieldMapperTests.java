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

package org.elasticsearch.percolator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsLookupQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PercolatorFieldMapperTests extends ESSingleNodeTestCase {

    private String typeName;
    private String fieldName;
    private IndexService indexService;
    private MapperService mapperService;
    private PercolatorFieldMapper.PercolatorFieldType fieldType;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    @Before
    public void init() throws Exception {
        indexService = createIndex("test", Settings.EMPTY);
        mapperService = indexService.mapperService();

        String mapper = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .startObject("number_field").field("type", "long").endObject()
                .startObject("date_field").field("type", "date").endObject()
            .endObject().endObject().endObject().string();
        mapperService.merge("type", new CompressedXContent(mapper), MapperService.MergeReason.MAPPING_UPDATE, true);
    }

    private void addQueryMapping() throws Exception {
        typeName = randomAsciiOfLength(4);
        fieldName = randomAsciiOfLength(4);
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(typeName)
                .startObject("properties").startObject(fieldName).field("type", "percolator").endObject().endObject()
                .endObject().endObject().string();
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);
        fieldType = (PercolatorFieldMapper.PercolatorFieldType) mapperService.fullName(fieldName);
    }

    public void testPercolatorFieldMapper() throws Exception {
        addQueryMapping();
        QueryBuilder queryBuilder = termQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper(typeName).parse("test", typeName, "1", XContentFactory.jsonBuilder().startObject()
            .field(fieldName, queryBuilder)
            .endObject().bytes());

        assertThat(doc.rootDoc().getFields(fieldType.getUnknownQueryFieldName()).length, equalTo(0));
        assertThat(doc.rootDoc().getFields(fieldType.getExtractedTermsField()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.getExtractedTermsField())[0].binaryValue().utf8ToString(), equalTo("field\0value"));
        assertThat(doc.rootDoc().getFields(fieldType.getQueryBuilderFieldName()).length, equalTo(1));
        BytesRef qbSource = doc.rootDoc().getFields(fieldType.getQueryBuilderFieldName())[0].binaryValue();
        assertQueryBuilder(qbSource, queryBuilder);

        // add an query for which we don't extract terms from
        queryBuilder = matchAllQuery();
        doc = mapperService.documentMapper(typeName).parse("test", typeName, "1", XContentFactory.jsonBuilder().startObject()
                .field(fieldName, queryBuilder)
                .endObject().bytes());
        assertThat(doc.rootDoc().getFields(fieldType.getUnknownQueryFieldName()).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(fieldType.getUnknownQueryFieldName())[0].binaryValue(), equalTo(new BytesRef()));
        assertThat(doc.rootDoc().getFields(fieldType.getExtractedTermsField()).length, equalTo(0));
        assertThat(doc.rootDoc().getFields(fieldType.getQueryBuilderFieldName()).length, equalTo(1));
        qbSource = doc.rootDoc().getFields(fieldType.getQueryBuilderFieldName())[0].binaryValue();
        assertQueryBuilder(qbSource, queryBuilder);
    }

    public void testStoringQueries() throws Exception {
        addQueryMapping();
        QueryBuilder[] queries = new QueryBuilder[]{
                termQuery("field", "value"), matchAllQuery(), matchQuery("field", "value"), matchPhraseQuery("field", "value"),
                prefixQuery("field", "v"), wildcardQuery("field", "v*"), rangeQuery("number_field").gte(0).lte(9),
                rangeQuery("date_field").from("2015-01-01T00:00").to("2015-01-01T00:00")
        };
        // note: it important that range queries never rewrite, otherwise it will cause results to be wrong.
        // (it can't use shard data for rewriting purposes, because percolator queries run on MemoryIndex)

        for (QueryBuilder query : queries) {
            ParsedDocument doc = mapperService.documentMapper(typeName).parse("test", typeName, "1",
                    XContentFactory.jsonBuilder().startObject()
                    .field(fieldName, query)
                    .endObject().bytes());
            BytesRef qbSource = doc.rootDoc().getFields(fieldType.getQueryBuilderFieldName())[0].binaryValue();
            assertQueryBuilder(qbSource, query);
        }
    }

    public void testQueryWithRewrite() throws Exception {
        addQueryMapping();
        client().prepareIndex("remote", "type", "1").setSource("field", "value").get();
        QueryBuilder queryBuilder = termsLookupQuery("field", new TermsLookup("remote", "type", "1", "field"));
        ParsedDocument doc = mapperService.documentMapper(typeName).parse("test", typeName, "1", XContentFactory.jsonBuilder().startObject()
                .field(fieldName, queryBuilder)
                .endObject().bytes());
        BytesRef qbSource = doc.rootDoc().getFields(fieldType.getQueryBuilderFieldName())[0].binaryValue();
        assertQueryBuilder(qbSource, queryBuilder.rewrite(indexService.newQueryShardContext()));
    }


    public void testPercolatorFieldMapperUnMappedField() throws Exception {
        addQueryMapping();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> {
            mapperService.documentMapper(typeName).parse("test", typeName, "1", XContentFactory.jsonBuilder().startObject()
                    .field(fieldName, termQuery("unmapped_field", "value"))
                    .endObject().bytes());
        });
        assertThat(exception.getCause(), instanceOf(QueryShardException.class));
        assertThat(exception.getCause().getMessage(), equalTo("No field mapping can be found for the field with name [unmapped_field]"));
    }


    public void testPercolatorFieldMapper_noQuery() throws Exception {
        addQueryMapping();
        ParsedDocument doc = mapperService.documentMapper(typeName).parse("test", typeName, "1", XContentFactory.jsonBuilder().startObject()
            .endObject().bytes());
        assertThat(doc.rootDoc().getFields(fieldType.getQueryBuilderFieldName()).length, equalTo(0));

        try {
            mapperService.documentMapper(typeName).parse("test", typeName, "1", XContentFactory.jsonBuilder().startObject()
                .nullField(fieldName)
                .endObject().bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("query malformed, must start with start_object"));
        }
    }

    public void testAllowNoAdditionalSettings() throws Exception {
        addQueryMapping();
        IndexService indexService = createIndex("test1", Settings.EMPTY);
        MapperService mapperService = indexService.mapperService();

        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(typeName)
            .startObject("properties").startObject(fieldName).field("type", "percolator").field("index", "no").endObject().endObject()
            .endObject().endObject().string();
        try {
            mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);
            fail("MapperParsingException expected");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("Mapping definition for [" + fieldName + "] has unsupported parameters:  [index : no]"));
        }
    }

    // multiple percolator fields are allowed in the mapping, but only one field can be used at index time.
    public void testMultiplePercolatorFields() throws Exception {
        String typeName = "another_type";
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(typeName)
                .startObject("properties")
                    .startObject("query_field1").field("type", "percolator").endObject()
                    .startObject("query_field2").field("type", "percolator").endObject()
                .endObject()
                .endObject().endObject().string();
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper(typeName).parse("test", typeName, "1",
                jsonBuilder().startObject()
                        .field("query_field1", queryBuilder)
                        .field("query_field2", queryBuilder)
                        .endObject().bytes()
        );
        assertThat(doc.rootDoc().getFields().size(), equalTo(22)); // also includes all other meta fields
        BytesRef queryBuilderAsBytes = doc.rootDoc().getField("query_field1.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        queryBuilderAsBytes = doc.rootDoc().getField("query_field2.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);
    }

    // percolator field can be nested under an object field, but only one query can be specified per document
    public void testNestedPercolatorField() throws Exception {
        String typeName = "another_type";
        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(typeName)
                .startObject("properties")
                .startObject("object_field")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("query_field").field("type", "percolator").endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();
        mapperService.merge(typeName, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);

        QueryBuilder queryBuilder = matchQuery("field", "value");
        ParsedDocument doc = mapperService.documentMapper(typeName).parse("test", typeName, "1",
                jsonBuilder().startObject().startObject("object_field")
                            .field("query_field", queryBuilder)
                        .endObject().endObject().bytes()
        );
        assertThat(doc.rootDoc().getFields().size(), equalTo(18)); // also includes all other meta fields
        BytesRef queryBuilderAsBytes = doc.rootDoc().getField("object_field.query_field.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        doc = mapperService.documentMapper(typeName).parse("test", typeName, "1",
                jsonBuilder().startObject()
                            .startArray("object_field")
                                .startObject().field("query_field", queryBuilder).endObject()
                            .endArray()
                        .endObject().bytes()
        );
        assertThat(doc.rootDoc().getFields().size(), equalTo(18)); // also includes all other meta fields
        queryBuilderAsBytes = doc.rootDoc().getField("object_field.query_field.query_builder_field").binaryValue();
        assertQueryBuilder(queryBuilderAsBytes, queryBuilder);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
                    mapperService.documentMapper(typeName).parse("test", typeName, "1",
                            jsonBuilder().startObject()
                                    .startArray("object_field")
                                        .startObject().field("query_field", queryBuilder).endObject()
                                        .startObject().field("query_field", queryBuilder).endObject()
                                    .endArray()
                                .endObject().bytes()
                    );
                }
        );
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), equalTo("a document can only contain one percolator query"));
    }

    private void assertQueryBuilder(BytesRef actual, QueryBuilder expected) throws IOException {
        XContentParser sourceParser = PercolatorFieldMapper.QUERY_BUILDER_CONTENT_TYPE.xContent()
                .createParser(actual.bytes, actual.offset, actual.length);
        QueryParseContext qsc = indexService.newQueryShardContext().newParseContext(sourceParser);
        assertThat(qsc.parseInnerQueryBuilder().get(), equalTo(expected));
    }
}
