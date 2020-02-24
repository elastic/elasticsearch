/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper.Builder;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import static org.hamcrest.Matchers.equalTo;

public class WildcardFieldMapperTests extends ESTestCase {

    private static final String KEYWORD_FIELD_NAME = "keyword_field";
    private static final String WILDCARD_FIELD_NAME = "wildcard_field";
    static WildcardFieldMapper wildcardFieldType;

    @Override
    @Before
    public void setUp() throws Exception {
        Builder builder = new WildcardFieldMapper.Builder(WILDCARD_FIELD_NAME);
        wildcardFieldType = builder.build(new Mapper.BuilderContext(createIndexSettings().getSettings(), new ContentPath(0)));
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
    
    public void testSearchResultsVersusKeywordField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        int numDocs = 100;
        HashSet<String> values = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String docContent = randomABString(1 + randomInt(MAX_FIELD_LENGTH));
            if (values.contains(docContent) == false) {
                createDocs(docContent, iw);
                values.add(docContent);
            }
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

            assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(kwTopDocs.totalHits.value));

            HashSet<Integer> expectedDocs = new HashSet<>();
            for (ScoreDoc topDoc : kwTopDocs.scoreDocs) {
                expectedDocs.add(topDoc.doc);
            }
            for (ScoreDoc wcTopDoc : wildcardFieldTopDocs.scoreDocs) {
                assertTrue(expectedDocs.remove(wcTopDoc.doc));
            }
            assertThat(expectedDocs.size(), equalTo(0));
        }
        reader.close();
        dir.close();
    }

    private void createDocs(String docContent, RandomIndexWriter iw) throws IOException {
        ArrayList<IndexableField> fields = new ArrayList<>();
        wildcardFieldType.createFields(docContent, fields);
        Document doc = new Document();
        for (IndexableField indexableField : fields) {
            doc.add(indexableField);
        }
        doc.add(new StringField(KEYWORD_FIELD_NAME, docContent, Field.Store.YES));
        iw.addDocument(doc);
    }

    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
                IndexMetaData.builder("_index").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                        .numberOfShards(1).numberOfReplicas(0).creationDate(System.currentTimeMillis()).build(),
                Settings.EMPTY);
    }

    static final int MAX_FIELD_LENGTH = 100;

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
