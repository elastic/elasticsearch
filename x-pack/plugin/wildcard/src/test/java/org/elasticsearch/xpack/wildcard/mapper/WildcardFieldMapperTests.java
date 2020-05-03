/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper.Builder;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;

public class WildcardFieldMapperTests extends ESTestCase {

    private static final String KEYWORD_FIELD_NAME = "keyword_field";
    private static final String WILDCARD_FIELD_NAME = "wildcard_field";
    static final int MAX_FIELD_LENGTH = 100;
    static WildcardFieldMapper wildcardFieldType;
    static KeywordFieldMapper keywordFieldType;

    @Override
    @Before
    public void setUp() throws Exception {
        Builder builder = new WildcardFieldMapper.Builder(WILDCARD_FIELD_NAME);
        builder.ignoreAbove(MAX_FIELD_LENGTH);
        wildcardFieldType = builder.build(new Mapper.BuilderContext(createIndexSettings().getSettings(), new ContentPath(0)));


        org.elasticsearch.index.mapper.KeywordFieldMapper.Builder kwBuilder = new KeywordFieldMapper.Builder(KEYWORD_FIELD_NAME);
        keywordFieldType = kwBuilder.build(new Mapper.BuilderContext(createIndexSettings().getSettings(), new ContentPath(0)));
        super.setUp();
    }

    public void testIllegalDocValuesArgument() {
        Builder ft = new WildcardFieldMapper.Builder("test");
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> ft.docValues(false));
        assertEquals("The field [test] cannot have doc values = false", e.getMessage());
    }

    public void testIllegalIndexedArgument() {
        Builder ft = new WildcardFieldMapper.Builder("test");
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> ft.index(false));
        assertEquals("The field [test] cannot have index = false", e.getMessage());
    }

    public void testTooBigKeywordField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        // Create a string that is too large and will not be indexed
        String docContent = randomABString(MAX_FIELD_LENGTH + 1);
        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
        addFields(parseDoc, doc, docContent);
        indexDoc(parseDoc, doc, iw);

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery("*a*", null, null);
        TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, 10, Sort.INDEXORDER);
        assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(0L));

        reader.close();
        dir.close();
    }

    //Test long query strings don't cause exceptions
    public void testTooBigQueryField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        // Create a string that is too large and will not be indexed
        String docContent = randomABString(10);
        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
        addFields(parseDoc, doc, docContent);
        indexDoc(parseDoc, doc, iw);

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        String queryString = randomABString((BooleanQuery.getMaxClauseCount() * 2) + 1);
        Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery(queryString, null, null);
        TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, 10, Sort.INDEXORDER);
        assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(0L));

        reader.close();
        dir.close();
    }


    public void testSearchResultsVersusKeywordField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        int numDocs = 100;
        HashSet<String> values = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            ParseContext.Document parseDoc = new ParseContext.Document();
            String docContent = randomABString(1 + randomInt(MAX_FIELD_LENGTH - 1));
            if (values.contains(docContent) == false) {
                addFields(parseDoc, doc, docContent);
                values.add(docContent);
            }
            // Occasionally add a multi-value field
            if (randomBoolean()) {
                docContent = randomABString(1 + randomInt(MAX_FIELD_LENGTH - 1));
                if (values.contains(docContent) == false) {
                    addFields(parseDoc, doc, docContent);
                    values.add(docContent);
                }
            }
            indexDoc(parseDoc, doc, iw);

        }

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        int numSearches = 100;
        for (int i = 0; i < numSearches; i++) {
            String randomWildcardPattern = getRandomWildcardPattern();

            Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery(randomWildcardPattern, null, null);
            TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, values.size() + 1, Sort.INDEXORDER);

            Query keywordFieldQuery = new WildcardQuery(new Term(KEYWORD_FIELD_NAME, randomWildcardPattern));
            TopDocs kwTopDocs = searcher.search(keywordFieldQuery, values.size() + 1, Sort.INDEXORDER);

            assertThat(kwTopDocs.totalHits.value, equalTo(wildcardFieldTopDocs.totalHits.value));

            HashSet<Integer> expectedDocs = new HashSet<>();
            for (ScoreDoc topDoc : kwTopDocs.scoreDocs) {
                expectedDocs.add(topDoc.doc);
            }
            for (ScoreDoc wcTopDoc : wildcardFieldTopDocs.scoreDocs) {
                assertTrue(expectedDocs.remove(wcTopDoc.doc));
            }
            assertThat(expectedDocs.size(), equalTo(0));
        }


        //Test keyword and wildcard sort operations are also equivalent
        QueryShardContext shardContextMock = createMockShardContext();

        FieldSortBuilder wildcardSortBuilder = new FieldSortBuilder(WILDCARD_FIELD_NAME);
        SortField wildcardSortField = wildcardSortBuilder.build(shardContextMock).field;
        ScoreDoc[] wildcardHits = searcher.search(new MatchAllDocsQuery(), numDocs, new Sort(wildcardSortField)).scoreDocs;

        FieldSortBuilder keywordSortBuilder = new FieldSortBuilder(KEYWORD_FIELD_NAME);
        SortField keywordSortField = keywordSortBuilder.build(shardContextMock).field;
        ScoreDoc[] keywordHits = searcher.search(new MatchAllDocsQuery(), numDocs, new Sort(keywordSortField)).scoreDocs;

        assertThat(wildcardHits.length, equalTo(keywordHits.length));
        for (int i = 0; i < wildcardHits.length; i++) {
            assertThat(wildcardHits[i].doc, equalTo(keywordHits[i].doc));
        }

        reader.close();
        dir.close();
    }



    protected MappedFieldType provideMappedFieldType(String name) {
        if (name.equals(WILDCARD_FIELD_NAME)) {
            return wildcardFieldType.fieldType();
        } else {
            return keywordFieldType.fieldType();
        }
    }

    protected final QueryShardContext createMockShardContext() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 10), "_na_");
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(idxSettings, Mockito.mock(BitsetFilterCache.Listener.class));
        BiFunction<MappedFieldType, String, IndexFieldData<?>> indexFieldDataLookup = (fieldType, fieldIndexName) -> {
            IndexFieldData.Builder builder = fieldType.fielddataBuilder(fieldIndexName);
            return builder.build(idxSettings, fieldType, new IndexFieldDataCache.None(), null, null);
        };
        return new QueryShardContext(0, idxSettings, BigArrays.NON_RECYCLING_INSTANCE, bitsetFilterCache, indexFieldDataLookup,
                null, null, null, xContentRegistry(), null, null, null,
                () -> randomNonNegativeLong(), null, null, () -> true, null) {

            @Override
            public MappedFieldType fieldMapper(String name) {
                return provideMappedFieldType(name);
            }
        };
    }

    private void addFields(ParseContext.Document parseDoc, Document doc, String docContent) throws IOException {
        ArrayList<IndexableField> fields = new ArrayList<>();
        wildcardFieldType.createFields(docContent, parseDoc, fields);

        for (IndexableField indexableField : fields) {
            doc.add(indexableField);
        }
        // Add keyword fields too
        doc.add(new SortedSetDocValuesField(KEYWORD_FIELD_NAME, new BytesRef(docContent)));
        doc.add(new StringField(KEYWORD_FIELD_NAME, docContent, Field.Store.YES));
    }

    private void indexDoc(ParseContext.Document parseDoc, Document doc, RandomIndexWriter iw) throws IOException {
        IndexableField field = parseDoc.getByKey(wildcardFieldType.name());
        if (field != null) {
            doc.add(field);
        }
        iw.addDocument(doc);
    }

    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
                IndexMetadata.builder("_index").settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                        .numberOfShards(1).numberOfReplicas(0).creationDate(System.currentTimeMillis()).build(),
                Settings.EMPTY);
    }


    static String randomABString(int minLength) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < minLength) {
            if (randomBoolean()) {
                sb.append("a");
            } else {
                sb.append("b");
            }
        }
        return sb.toString();
    }

    private void randomSyntaxChar(StringBuilder sb) {
        switch (randomInt(3)) {
        case 0:
            sb.append(WildcardQuery.WILDCARD_CHAR);
            break;
        case 1:
            sb.append(WildcardQuery.WILDCARD_STRING);
            break;
        case 2:
            sb.append(WildcardQuery.WILDCARD_ESCAPE);
            sb.append(WildcardQuery.WILDCARD_STRING);
            break;
        case 3:
            sb.append(WildcardQuery.WILDCARD_ESCAPE);
            sb.append(WildcardQuery.WILDCARD_CHAR);
            break;
        }
    }

    private String getRandomWildcardPattern() {
        StringBuilder sb = new StringBuilder();
        int numFragments = 1 + randomInt(4);
        if (randomInt(10) == 1) {
            randomSyntaxChar(sb);
        }
        for (int i = 0; i < numFragments; i++) {
            if (i > 0) {
                randomSyntaxChar(sb);
            }
            sb.append(randomABString(1 + randomInt(6)));
        }
        if (randomInt(10) == 1) {
            randomSyntaxChar(sb);
        }
        return sb.toString();
    }
}
