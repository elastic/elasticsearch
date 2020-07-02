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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldType;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.plugins.Plugin;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasProperty;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class SearchAsYouTypeFieldMapperTests extends FieldMapperTestCase<SearchAsYouTypeFieldMapper.Builder> {

    @Before
    public void addModifiers() {
        addModifier("max_shingle_size", false, (a, b) -> {
            a.maxShingleSize(3);
            b.maxShingleSize(2);
        });
        addModifier("similarity", false, (a, b) -> {
            a.similarity(new SimilarityProvider("BM25", new BM25Similarity()));
            b.similarity(new SimilarityProvider("boolean", new BooleanSimilarity()));
        });
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("doc_values");
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    @Override
    protected SearchAsYouTypeFieldMapper.Builder newBuilder() {
        return new SearchAsYouTypeFieldMapper.Builder("sayt")
            .indexAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()))
            .searchAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()))
            .searchQuoteAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
    }

    public void testIndexing() throws IOException {
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
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
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
        assertRootFieldMapper(rootMapper, 3, "default");


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
        assertRootFieldMapper(rootMapper, maxShingleSize, analyzerName);

        final PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "a_field._index_prefix");
        assertPrefixFieldType(prefixFieldMapper.fieldType(), maxShingleSize, analyzerName);

        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._2gram").fieldType(), 2, analyzerName, prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._3gram").fieldType(), 3, analyzerName, prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "a_field._4gram").fieldType(), 4, analyzerName, prefixFieldMapper.fieldType());
    }

    public void testSimpleMerge() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "search_as_you_type")
                            .field("analyzer", "standard")
                        .endObject()
                    .endObject()
                .endObject().endObject());
            DocumentMapper mapper = mapperService.merge("_doc",
                new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        }

        {
           String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "search_as_you_type")
                            .field("analyzer", "standard")
                        .endObject()
                        .startObject("b_field")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject());
            DocumentMapper mapper = mapperService.merge("_doc",
                new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "search_as_you_type")
                            .field("analyzer", "standard")
                            .field("max_shingle_size", "4")
                        .endObject()
                        .startObject("b_field")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("_doc",
                    new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(e.getMessage(), containsString("different [max_shingle_size]"));
        }
    }

    public void testMultiFields() throws IOException {
        for (int shingleSize = 2; shingleSize < 4; shingleSize++) {
            final XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "text")
                            .startObject("fields")
                                .startObject("suggest")
                                    .field("type", "search_as_you_type")
                                    .field("max_shingle_size", shingleSize)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

            final String index = "foo_" + shingleSize;
            final String path = "a_field.suggest";
            List<String> fields = new ArrayList<>();
            fields.add(path);
            final MapperService mapperService =
                createIndex(index, Settings.EMPTY, mapping).mapperService();
            MappedFieldType fieldType = mapperService.fieldType(path + "._index_prefix");
            assertThat(fieldType, instanceOf(PrefixFieldType.class));
            PrefixFieldType prefixFieldType = (PrefixFieldType) fieldType;
            assertEquals(path, prefixFieldType.parentField);
            for (int i = 2; i < shingleSize; i++) {
                String name = path + "._" + i + "gram";
                fields.add(name);
                fieldType = mapperService.fieldType(name);
                assertThat(fieldType, instanceOf(ShingleFieldType.class));
                ShingleFieldType ft = (ShingleFieldType) fieldType;
                assertEquals(i, ft.shingleSize);
                assertTrue(prefixFieldType == ft.prefixFieldType);
            }

            ParsedDocument doc = mapperService.documentMapper()
                .parse(new SourceToParse("test", "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                                .field("a_field", "new york city")
                            .endObject()
                    ), XContentType.JSON)
                );
            for (String field : fields) {
                IndexableField[] indexFields = doc.rootDoc().getFields(field);
                assertEquals(1, indexFields.length);
                assertEquals("new york city", indexFields[0].stringValue());
            }
        }
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
        ).forEach(mapper -> assertThat("for " + mapper.name(),
            mapper.fieldType.indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)));
    }

    public void testStore() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("a_field")
                        .field("type", "search_as_you_type")
                        .field("store", "true")
                    .endObject()
                .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        assertTrue(getRootFieldMapper(defaultMapper, "a_field").fieldType.stored());
        Stream.of(
            getPrefixFieldMapper(defaultMapper, "a_field._index_prefix"),
            getShingleFieldMapper(defaultMapper, "a_field._2gram"),
            getShingleFieldMapper(defaultMapper, "a_field._3gram")
        ).forEach(mapper -> assertFalse("for " + mapper.name(), mapper.fieldType.stored()));
    }

    public void testIndex() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("a_field")
                        .field("type", "search_as_you_type")
                        .field("index", "false")
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
        ).forEach(mapper -> assertThat("for " + mapper.name(), mapper.fieldType.indexOptions(), equalTo(IndexOptions.NONE)));
    }

    public void testTermVectors() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("a_field")
                        .field("type", "search_as_you_type")
                        .field("term_vector", "yes")
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
            getShingleFieldMapper(defaultMapper, "a_field._2gram"),
            getShingleFieldMapper(defaultMapper, "a_field._3gram")
        ).forEach(mapper -> assertTrue("for " + mapper.name(), mapper.fieldType.storeTermVectors()));

        final PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "a_field._index_prefix");
        assertFalse(prefixFieldMapper.fieldType.storeTermVectors());
    }

    public void testNorms() throws IOException {
        // default setting
        {
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

            final DocumentMapper defaultMapper = createIndex("test-1")
                .mapperService()
                .documentMapperParser()
                .parse("_doc", new CompressedXContent(mapping));

            Stream.of(
                getRootFieldMapper(defaultMapper, "a_field"),
                getShingleFieldMapper(defaultMapper, "a_field._2gram"),
                getShingleFieldMapper(defaultMapper, "a_field._3gram")
            ).forEach(mapper -> assertFalse("for " + mapper.name(), mapper.fieldType.omitNorms()));

            final PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "a_field._index_prefix");
            assertTrue(prefixFieldMapper.fieldType.omitNorms());
        }

        // can disable them on shingle fields
        {
            final String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "search_as_you_type")
                            .field("norms", "false")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject());

            final DocumentMapper defaultMapper = createIndex("test-2")
                .mapperService()
                .documentMapperParser()
                .parse("_doc", new CompressedXContent(mapping));

            Stream.of(
                getRootFieldMapper(defaultMapper, "a_field"),
                getPrefixFieldMapper(defaultMapper, "a_field._index_prefix"),
                getShingleFieldMapper(defaultMapper, "a_field._2gram"),
                getShingleFieldMapper(defaultMapper, "a_field._3gram")
            ).forEach(mapper -> assertTrue("for " + mapper.name(), mapper.fieldType.omitNorms()));
        }
    }


    public void testDocumentParsingSingleValue() throws IOException {
        documentParsingTestCase(Collections.singleton(randomAlphaOfLengthBetween(5, 20)));
    }

    public void testDocumentParsingMultipleValues() throws IOException {
        documentParsingTestCase(randomUnique(() -> randomAlphaOfLengthBetween(3, 20), randomIntBetween(2, 10)));
    }

    public void testMatchPhrasePrefix() throws IOException {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
            randomInt(20), null, () -> {
                throw new UnsupportedOperationException();
            }, null);

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("field")
                    .field("type", "search_as_you_type")
                .endObject()
            .endObject()
            .endObject().endObject());

        queryShardContext.getMapperService().merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").toQuery(queryShardContext);
            Query expected = new SynonymQuery(new Term("field._index_prefix", "two words"));
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "three words here").toQuery(queryShardContext);
            Query expected = new SynonymQuery(new Term("field._index_prefix", "three words here"));
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").slop(1).toQuery(queryShardContext);
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("field");
            mpq.setSlop(1);
            mpq.add(new Term("field", "two"));
            mpq.add(new Term("field", "words"));
            assertThat(q, equalTo(mpq));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "more than three words").toQuery(queryShardContext);
            Query expected = new SpanNearQuery.Builder("field._3gram", true)
                .addClause(new SpanTermQuery(new Term("field._3gram", "more than three")))
                .addClause(new FieldMaskingSpanQuery(
                    new SpanTermQuery(new Term("field._index_prefix", "than three words")), "field._3gram")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field._3gram", "more than three words").toQuery(queryShardContext);
            Query expected = new SpanNearQuery.Builder("field._3gram", true)
                .addClause(new SpanTermQuery(new Term("field._3gram", "more than three")))
                .addClause(new FieldMaskingSpanQuery(
                    new SpanTermQuery(new Term("field._index_prefix", "than three words")), "field._3gram")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field._3gram", "two words").toQuery(queryShardContext);
            Query expected = new MatchNoDocsQuery();
            assertThat(q, equalTo(expected));
        }

        {
            Query actual = new MatchPhrasePrefixQueryBuilder("field._3gram", "one two three four")
                .slop(1)
                .toQuery(queryShardContext);
            MultiPhrasePrefixQuery expected = new MultiPhrasePrefixQuery("field._3gram");
            expected.setSlop(1);
            expected.add(new Term("field._3gram", "one two three"));
            expected.add(new Term("field._3gram", "two three four"));
            assertThat(actual, equalTo(expected));
        }

    }

    public void testMatchPhrase() throws IOException {
        final IndexService indexService = createIndex("test", Settings.EMPTY);
        final QueryShardContext queryShardContext = indexService.newQueryShardContext(randomInt(20), null,
            () -> { throw new UnsupportedOperationException(); }, null);
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

        queryShardContext.getMapperService().merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field", "one")
                .toQuery(queryShardContext);
            final Query expected = new TermQuery(new Term("a_field", "one"));
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field", "one two")
                .toQuery(queryShardContext);
            final Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("a_field._2gram", "one two"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field", "one two three")
                .toQuery(queryShardContext);
            final Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("a_field._3gram", "one two three"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field", "one two three four")
                .toQuery(queryShardContext);
            final Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("a_field._3gram", "one two three"))
                .add(new Term("a_field._3gram", "two three four"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field", "one two")
                .slop(1)
                .toQuery(queryShardContext);
            final Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("a_field", "one"))
                .add(new Term("a_field", "two"))
                .setSlop(1)
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field._2gram", "one two")
                .toQuery(queryShardContext);
            final Query expected = new TermQuery(new Term("a_field._2gram", "one two"));
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field._2gram", "one two three")
                .toQuery(queryShardContext);
            final Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("a_field._2gram", "one two"))
                .add(new Term("a_field._2gram", "two three"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field._3gram", "one two three")
                .toQuery(queryShardContext);
            final Query expected = new TermQuery(new Term("a_field._3gram", "one two three"));
            assertThat(actual, equalTo(expected));
        }

        {
            final Query actual = new MatchPhraseQueryBuilder("a_field._3gram", "one two three four")
                .toQuery(queryShardContext);
            final Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("a_field._3gram", "one two three"))
                .add(new Term("a_field._3gram", "two three four"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            expectThrows(IllegalArgumentException.class,
                () -> new MatchPhraseQueryBuilder("a_field._index_prefix", "one two three four").toQuery(queryShardContext));
        }
    }

    private static BooleanQuery buildBoolPrefixQuery(String shingleFieldName, String prefixFieldName, List<String> terms) {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int i = 0; i < terms.size() - 1; i++) {
            final String term = terms.get(i);
            builder.add(new BooleanClause(new TermQuery(new Term(shingleFieldName, term)), BooleanClause.Occur.SHOULD));
        }
        final String finalTerm = terms.get(terms.size() - 1);
        builder.add(new BooleanClause(
            new ConstantScoreQuery(new TermQuery(new Term(prefixFieldName, finalTerm))), BooleanClause.Occur.SHOULD));
        return builder.build();
    }

    public void testMultiMatchBoolPrefix() throws IOException {
        final IndexService indexService = createIndex("test", Settings.EMPTY);
        final QueryShardContext queryShardContext = indexService.newQueryShardContext(randomInt(20), null,
            () -> { throw new UnsupportedOperationException(); }, null);
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "search_as_you_type")
                            .field("max_shingle_size", 4)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

        queryShardContext.getMapperService().merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        final MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder(
            "quick brown fox jump lazy dog",
            "a_field",
            "a_field._2gram",
            "a_field._3gram",
            "a_field._4gram"
        );
        builder.type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);

        final Query actual = builder.toQuery(queryShardContext);
        assertThat(actual, instanceOf(DisjunctionMaxQuery.class));
        final DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) actual;
        assertThat(disMaxQuery.getDisjuncts(), hasSize(4));
        assertThat(disMaxQuery.getDisjuncts(), containsInAnyOrder(
            buildBoolPrefixQuery(
                "a_field", "a_field._index_prefix", asList("quick", "brown", "fox", "jump", "lazy", "dog")),
            buildBoolPrefixQuery("a_field._2gram", "a_field._index_prefix",
                asList("quick brown", "brown fox", "fox jump", "jump lazy", "lazy dog")),
            buildBoolPrefixQuery("a_field._3gram", "a_field._index_prefix",
                asList("quick brown fox", "brown fox jump", "fox jump lazy", "jump lazy dog")),
            buildBoolPrefixQuery("a_field._4gram", "a_field._index_prefix",
                asList("quick brown fox jump", "brown fox jump lazy", "fox jump lazy dog"))));
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
            new SourceToParse("test", "1", BytesReference.bytes(builder), XContentType.JSON));

        IndexableField[] rootFields = parsedDocument.rootDoc().getFields("a_field");
        IndexableField[] prefixFields = parsedDocument.rootDoc().getFields("a_field._index_prefix");
        IndexableField[] shingle2Fields = parsedDocument.rootDoc().getFields("a_field._2gram");
        IndexableField[] shingle3Fields = parsedDocument.rootDoc().getFields("a_field._3gram");
        for (IndexableField[] fields : new IndexableField[][]{ rootFields, prefixFields, shingle2Fields, shingle3Fields}) {
            Set<String> expectedValues = Arrays.stream(fields).map(IndexableField::stringValue).collect(Collectors.toSet());
            assertThat(values, equalTo(expectedValues));
        }
    }

    private static Matcher<IndexableField> indexableFieldMatcher(String value, Class<? extends FieldType> fieldTypeClass) {
        return Matchers.allOf(
            hasProperty(IndexableField::stringValue, equalTo(value)),
            hasProperty(IndexableField::fieldType, instanceOf(fieldTypeClass))
        );
    }

    private static void assertRootFieldMapper(SearchAsYouTypeFieldMapper mapper,
                                              int maxShingleSize,
                                              String analyzerName) {

        assertThat(mapper.maxShingleSize(), equalTo(maxShingleSize));
        assertThat(mapper.fieldType(), notNullValue());
        assertSearchAsYouTypeFieldType(mapper.fieldType(), maxShingleSize, analyzerName, mapper.prefixField().fieldType());

        assertThat(mapper.prefixField(), notNullValue());
        assertThat(mapper.prefixField().fieldType().parentField, equalTo(mapper.name()));
        assertPrefixFieldType(mapper.prefixField().fieldType(), maxShingleSize, analyzerName);


        for (int shingleSize = 2; shingleSize <= maxShingleSize; shingleSize++) {
            final ShingleFieldMapper shingleFieldMapper = mapper.shingleFields()[shingleSize - 2];
            assertThat(shingleFieldMapper, notNullValue());
            assertShingleFieldType(shingleFieldMapper.fieldType(), shingleSize, analyzerName, mapper.prefixField().fieldType());
        }

        final int numberOfShingleSubfields = (maxShingleSize - 2) + 1;
        assertThat(mapper.shingleFields().length, equalTo(numberOfShingleSubfields));
    }

    private static void assertSearchAsYouTypeFieldType(SearchAsYouTypeFieldType fieldType, int maxShingleSize,
                                                       String analyzerName,
                                                       PrefixFieldType prefixFieldType) {

        assertThat(fieldType.shingleFields.length, equalTo(maxShingleSize-1));
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.getTextSearchInfo().getSearchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }
        int shingleSize = 2;
        for (ShingleFieldType shingleField : fieldType.shingleFields) {
            assertShingleFieldType(shingleField, shingleSize++, analyzerName, prefixFieldType);
        }

        assertThat(fieldType.prefixField, equalTo(prefixFieldType));
    }

    private static void assertShingleFieldType(ShingleFieldType fieldType,
                                               int shingleSize,
                                               String analyzerName,
                                               PrefixFieldType prefixFieldType) {

        assertThat(fieldType.shingleSize, equalTo(shingleSize));

        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.getTextSearchInfo().getSearchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
            if (shingleSize > 1) {
                final SearchAsYouTypeAnalyzer wrappedAnalyzer = (SearchAsYouTypeAnalyzer) analyzer.analyzer();
                assertThat(wrappedAnalyzer.shingleSize(), equalTo(shingleSize));
                assertThat(wrappedAnalyzer.indexPrefixes(), equalTo(false));
            }
        }

        assertThat(fieldType.prefixFieldType, equalTo(prefixFieldType));

    }

    private static void assertPrefixFieldType(PrefixFieldType fieldType, int shingleSize, String analyzerName) {
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.getTextSearchInfo().getSearchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }

        final SearchAsYouTypeAnalyzer wrappedIndexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();
        final SearchAsYouTypeAnalyzer wrappedSearchAnalyzer
            = (SearchAsYouTypeAnalyzer) fieldType.getTextSearchInfo().getSearchAnalyzer().analyzer();
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
