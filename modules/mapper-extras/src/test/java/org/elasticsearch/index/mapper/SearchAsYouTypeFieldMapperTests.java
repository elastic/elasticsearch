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

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;

public class SearchAsYouTypeFieldMapperTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    public void testIndex() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("a_field")
                        .field("type", "search_as_you_type")
                    .endObject()
                .endObject()
            .endObject()
            .endObject());

        final DocumentMapper mapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "_doc", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("a_field", "new york city")
                .endObject()),
            XContentType.JSON));

        for (String field : new String[] { "a_field", "a_field._index_prefix", "a_field._2gram", "a_field._3gram"}) {
            IndexableField[] fields = doc.rootDoc().getFields(field);
            assertEquals(1, fields.length);
            assertEquals("new york city", fields[0].stringValue());
        }
    }

   /** public void testDefaultConfiguration() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a_field")
            .field("type", "search_as_you_type")
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        final SearchAsYouTypeFieldMapper rootMapper = getRootFieldMapper(defaultMapper, "a_field");
        assertRootFieldMapper(rootMapper, 2, 3, "default");

        final SubFieldMapper edgeNGramsMapper = rootMapper.subFieldMappers().withMaxShinglesAndEdgeNGrams;
        assertEdgeNGramsFieldMapper(edgeNGramsMapper, 3, "default");

        assertShingleSubFieldMapper(getSubFieldMapper(defaultMapper, "a_field._with_2_shingles"), 2, "default", edgeNGramsMapper);
        assertShingleSubFieldMapper(getSubFieldMapper(defaultMapper, "a_field._with_3_shingles"), 3, "default", edgeNGramsMapper);
    }

    public void testConfiguration() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a_field")
            .field("type", "search_as_you_type")
            .field("analyzer", "simple")
            .field("max_shingle_size", 4)
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        final SearchAsYouTypeFieldMapper rootMapper = getRootFieldMapper(defaultMapper, "a_field");
        assertRootFieldMapper(rootMapper, 2, 4, "simple");

        final SubFieldMapper edgeNGramsMapper = rootMapper.subFieldMappers().withMaxShinglesAndEdgeNGrams;
        assertEdgeNGramsFieldMapper(edgeNGramsMapper, 4, "simple");

        assertShingleSubFieldMapper(getSubFieldMapper(defaultMapper, "a_field._with_2_shingles"), 2, "simple", edgeNGramsMapper);
        assertShingleSubFieldMapper(getSubFieldMapper(defaultMapper, "a_field._with_3_shingles"), 3, "simple", edgeNGramsMapper);
        assertShingleSubFieldMapper(getSubFieldMapper(defaultMapper, "a_field._with_4_shingles"), 4, "simple", edgeNGramsMapper);
    }

    public void testDocumentParsingSingleValue() throws IOException {
        documentParsingTestCase(Collections.singleton(randomAlphaOfLengthBetween(5, 20)));
    }

    public void testDocumentParsingMultipleValues2() throws IOException {
        documentParsingTestCase(randomUnique(() -> randomAlphaOfLengthBetween(3, 20), randomIntBetween(2, 10)));
    }

    private void documentParsingTestCase(Collection<String> values) throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a_field")
            .field("type", "search_as_you_type")
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        if (values.size() > 1) {
            builder.array("a_field", values.toArray(new String[0]));
        } else {
            builder.field("a_field", values.iterator().next());
        }
        builder.endObject();
        final ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse("test", "_doc", "1", BytesReference.bytes(builder), XContentType.JSON));

        final Set<Matcher<IndexableField>> rootFieldMatchers = values.stream()
            .map(value -> indexableFieldMatcher(value, SearchAsYouTypeFieldType.class))
            .collect(Collectors.toSet());
        final Set<Matcher<IndexableField>> subFieldMatchers = values.stream()
            .map(value -> indexableFieldMatcher(value, SubFieldType.class))
            .collect(Collectors.toSet());

        // the new ArrayList<>() here is to avoid the varargs form of arrayContainingInAnyOrder
        assertThat(parsedDocument.rootDoc().getFields("a_field"), arrayContainingInAnyOrder(new ArrayList<>(rootFieldMatchers)));
        for (String name : asList("a_field._with_2_shingles", "a_field._with_3_shingles", "a_field._with_3_shingles_and_edge_ngrams")) {
            assertThat(parsedDocument.rootDoc().getFields(name), arrayContainingInAnyOrder(new ArrayList<>(subFieldMatchers)));
        }
    }

    private static Matcher<IndexableField> indexableFieldMatcher(String value, Class<? extends FieldType> fieldTypeClass) {
        return Matchers.allOf(
            hasProperty(IndexableField::stringValue, equalTo(value)),
            hasProperty(IndexableField::fieldType, instanceOf(fieldTypeClass))
        );
    }

    private static void assertRootFieldMapper(SearchAsYouTypeFieldMapper mapper,
                                              int minShingleSize,
                                              int maxShingleSize,
                                              String analyzerName) {

        final SearchAsYouTypeFieldType fieldType = mapper.fieldType();
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }

        assertThat(mapper.subFieldMappers().minShingleSize, equalTo(minShingleSize));
        assertThat(mapper.subFieldMappers().maxShingleSize, equalTo(maxShingleSize));

        final SubFieldMapper edgeNGramsFieldMapper = mapper.subFieldMappers().withMaxShinglesAndEdgeNGrams;
        assertThat(edgeNGramsFieldMapper, notNullValue());
        final SubFieldType edgeNGramsFieldType = fieldType.getSubFieldTypes().withMaxShinglesAndEdgeNGrams;
        assertThat(edgeNGramsFieldType, notNullValue());
        assertThat(edgeNGramsFieldType, equalTo(edgeNGramsFieldMapper.fieldType()));
        assertEdgeNGramsFieldMapper(edgeNGramsFieldMapper, maxShingleSize, analyzerName);

        for (int shingleSize = minShingleSize; shingleSize <= maxShingleSize; shingleSize++) {
            final SubFieldMapper subFieldMapper = mapper.subFieldMappers().withShingles.get(shingleSize);
            assertThat(subFieldMapper, notNullValue());
            final SubFieldType subFieldType = fieldType.getSubFieldTypes().withShingles.get(shingleSize);
            assertThat(subFieldType, notNullValue());
            assertThat(subFieldType, equalTo(subFieldMapper.fieldType()));
            assertShingleSubFieldMapper(subFieldMapper, shingleSize, analyzerName, edgeNGramsFieldMapper);
        }

        final int numberOfShingleSubfields = (maxShingleSize - minShingleSize) + 1;
        assertThat(mapper.subFieldMappers().withShingles.entrySet(), hasSize(numberOfShingleSubfields));
        assertThat(fieldType.getSubFieldTypes().withShingles.entrySet(), hasSize(numberOfShingleSubfields));
    }

    private static void assertShingleSubFieldMapper(SubFieldMapper mapper,
                                                    int shingleSize,
                                                    String analyzerName,
                                                    SubFieldMapper edgeNGramsMapper) {

        final SubFieldType fieldType = mapper.fieldType();
        for (NamedAnalyzer namedAnalyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(namedAnalyzer.name(), equalTo(analyzerName));
            assertThat(namedAnalyzer.analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
            final SearchAsYouTypeAnalyzer analyzer = (SearchAsYouTypeAnalyzer) namedAnalyzer.analyzer();
            assertThat(analyzer.shingleSize(), equalTo(shingleSize));
            assertThat(analyzer.hasEdgeNGrams(), equalTo(false));
        }

        assertThat(mapper.edgeNGramsFieldMapper(), notNullValue());
        assertThat(mapper.edgeNGramsFieldMapper(), equalTo(edgeNGramsMapper));
        assertThat(fieldType.edgeNGramsField(), notNullValue());
        assertThat(fieldType.edgeNGramsField(), equalTo(edgeNGramsMapper.fieldType()));
        assertThat(fieldType.shingleSize(), equalTo(shingleSize));
        assertThat(fieldType.hasEdgeNGrams(), equalTo(false));
    }

    private static void assertEdgeNGramsFieldMapper(SubFieldMapper mapper, int shingleSize, String analyzerName) {
        final SubFieldType fieldType = mapper.fieldType();
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }
        assertThat(fieldType.indexAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        final SearchAsYouTypeAnalyzer indexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();
        assertThat(fieldType.searchAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        final SearchAsYouTypeAnalyzer searchAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.searchAnalyzer().analyzer();
        for (SearchAsYouTypeAnalyzer analyzer : asList(indexAnalyzer, searchAnalyzer)) {
            assertThat(analyzer.shingleSize(), equalTo(shingleSize));
        }
        assertThat(indexAnalyzer.hasEdgeNGrams(), equalTo(true));
        assertThat(searchAnalyzer.hasEdgeNGrams(), equalTo(false));

        assertThat(mapper.edgeNGramsFieldMapper(), nullValue());
        assertThat(fieldType.edgeNGramsField(), nullValue());
        assertThat(fieldType.shingleSize(), equalTo(shingleSize));
        assertThat(fieldType.hasEdgeNGrams(), equalTo(true));
    }

    private static SearchAsYouTypeFieldMapper getRootFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(SearchAsYouTypeFieldMapper.class));
        return (SearchAsYouTypeFieldMapper) mapper;
    }

    private static SubFieldMapper getSubFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(SubFieldMapper.class));
        return (SubFieldMapper) mapper;
    }
**/

}
