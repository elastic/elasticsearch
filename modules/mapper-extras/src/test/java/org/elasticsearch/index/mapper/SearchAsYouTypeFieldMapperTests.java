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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasProperty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsArrayContainingInAnyOrder.arrayContainingInAnyOrder;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

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

   public void testDefaultConfiguration() throws IOException {
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


        final PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "a_field._index_prefix");
        assertPrefixFieldType(prefixFieldMapper.fieldType(), 3, "default");

        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._2gram").fieldType(), 2, "default", prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._3gram").fieldType(), 3, "default", prefixFieldMapper.fieldType());
    }

    public void testConfiguration() throws IOException {
        final int maxShingleSize = 4;
        final String analyzerName = "simple";

        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("a_field")
                        .field("type", "search_as_you_type")
                        .field("analyzer", analyzerName)
                        .field("max_shingle_size", maxShingleSize)
                    .endObject()
                .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        final SearchAsYouTypeFieldMapper rootMapper = getRootFieldMapper(defaultMapper, "a_field");
        assertRootFieldMapper(rootMapper, 2, maxShingleSize, analyzerName);

        final PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "a_field._index_prefix");
        assertPrefixFieldType(prefixFieldMapper.fieldType(), maxShingleSize, analyzerName);

        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._2gram").fieldType(), 2, analyzerName, prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._3gram").fieldType(), 3, analyzerName, prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._4gram").fieldType(), 4, analyzerName, prefixFieldMapper.fieldType());
    }

    public void testIndexOptions() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("a_field")
                        .field("type", "search_as_you_type")
                        .field("index_options", "offsets")
                    .endObject()
                .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Stream.of(
            getRootFieldMapper(defaultMapper, "a_field"),
            getPrefixFieldMapper(defaultMapper, "a_field._index_prefix"),
            getShingleFieldMapper(defaultMapper, "a_field._2gram"),
            getShingleFieldMapper(defaultMapper, "a_field._3gram")
        ).forEach(mapper -> assertThat("index options for " + mapper.name() + " is configurable",
            mapper.fieldType().indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)));
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

        final Set<Matcher<IndexableField>> shingleFieldMatchers = values.stream()
            .map(value -> indexableFieldMatcher(value, ShingleFieldType.class))
            .collect(Collectors.toSet());
        final Set<Matcher<IndexableField>> prefixFieldMatchers = values.stream()
            .map(value -> indexableFieldMatcher(value, PrefixFieldType.class))
            .collect(Collectors.toSet());

        // the use of new ArrayList<>() here is to avoid the varargs form of arrayContainingInAnyOrder
        assertThat(
            parsedDocument.rootDoc().getFields("a_field._index_prefix"),
            arrayContainingInAnyOrder(new ArrayList<>(prefixFieldMatchers)));

        for (String name : asList("a_field", "a_field._2gram", "a_field._3gram")) {
            assertThat(parsedDocument.rootDoc().getFields(name), arrayContainingInAnyOrder(new ArrayList<>(shingleFieldMatchers)));
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

        assertThat(mapper.maxShingleSize(), equalTo(maxShingleSize));
        assertThat(mapper.fieldType(), notNullValue());
        assertShingleFieldType(mapper.fieldType(), 1, analyzerName, mapper.prefixField().fieldType());

        assertThat(mapper.prefixField(), notNullValue());
        assertThat(mapper.prefixField().fieldType().parentField, equalTo(mapper.name()));
        assertPrefixFieldType(mapper.prefixField().fieldType(), maxShingleSize, analyzerName);


        for (int shingleSize = minShingleSize; shingleSize <= maxShingleSize; shingleSize++) {
            final ShingleFieldMapper shingleFieldMapper = mapper.shingleFields().get(shingleSize - 2);
            assertThat(shingleFieldMapper, notNullValue());
            assertShingleFieldType(shingleFieldMapper.fieldType(), shingleSize, analyzerName, mapper.prefixField().fieldType());
        }

        final int numberOfShingleSubfields = (maxShingleSize - minShingleSize) + 1;
        assertThat(mapper.shingleFields(), hasSize(numberOfShingleSubfields));
    }

    private static void assertShingleFieldType(ShingleFieldType fieldType,
                                               int shingleSize,
                                               String analyzerName,
                                               PrefixFieldType prefixFieldType) {

        assertThat(fieldType.getShingleSize(), equalTo(shingleSize));

        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
            if (shingleSize > 1) {
                final SearchAsYouTypeAnalyzer wrappedAnalyzer = (SearchAsYouTypeAnalyzer) analyzer.analyzer();
                assertThat(wrappedAnalyzer.shingleSize(), equalTo(shingleSize));
                assertThat(wrappedAnalyzer.indexPrefixes(), equalTo(false));
            }
        }

        assertThat(fieldType.getPrefixFieldType(), equalTo(prefixFieldType));

    }

    private static void assertPrefixFieldType(PrefixFieldType fieldType, int shingleSize, String analyzerName) {
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }

        final SearchAsYouTypeAnalyzer wrappedIndexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();
        final SearchAsYouTypeAnalyzer wrappedSearchAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.searchAnalyzer().analyzer();
        for (SearchAsYouTypeAnalyzer analyzer : asList(wrappedIndexAnalyzer, wrappedSearchAnalyzer)) {
            assertThat(analyzer.shingleSize(), equalTo(shingleSize));
        }
        assertThat(wrappedIndexAnalyzer.indexPrefixes(), equalTo(true));
        assertThat(wrappedSearchAnalyzer.indexPrefixes(), equalTo(false));
    }

    private static SearchAsYouTypeFieldMapper getRootFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(SearchAsYouTypeFieldMapper.class));
        return (SearchAsYouTypeFieldMapper) mapper;
    }

    private static ShingleFieldMapper getShingleFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(ShingleFieldMapper.class));
        return (ShingleFieldMapper) mapper;
    }

    private static PrefixFieldMapper getPrefixFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(PrefixFieldMapper.class));
        return (PrefixFieldMapper) mapper;
    }
}
