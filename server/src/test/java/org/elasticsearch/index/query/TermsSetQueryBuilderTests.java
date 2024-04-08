/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CoveringQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TermsSetQueryBuilderTests extends AbstractQueryTestCase<TermsSetQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(CustomScriptPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        String docType = "_doc";
        mapperService.merge(
            docType,
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping("m_s_m", "type=long"))),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected TermsSetQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomValueOtherThanMany(value -> value.equals(GEO_POINT_FIELD_NAME), () -> randomFrom(MAPPED_FIELD_NAMES));
        List<?> randomTerms = randomValues(fieldName);
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, randomTerms);
        switch (randomIntBetween(0, 2)) {
            case 0 -> queryBuilder.setMinimumShouldMatchField("m_s_m");
            case 1 -> queryBuilder.setMinimumShouldMatchScript(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap()));
            case 2 -> queryBuilder.setMinimumShouldMatch("2");
        }
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(TermsSetQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (queryBuilder.getValues().isEmpty()) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
            MatchNoDocsQuery matchNoDocsQuery = (MatchNoDocsQuery) query;
            assertThat(matchNoDocsQuery.toString(), containsString("No terms supplied for \"terms_set\" query."));
        } else {
            assertThat(query, instanceOf(CoveringQuery.class));
        }
    }

    /**
     * Check that this query is generally not cacheable and explicitly testing the two conditions when it is not as well
     */
    @Override
    public void testCacheability() throws IOException {
        TermsSetQueryBuilder queryBuilder = createTestQueryBuilder();
        boolean isCacheable = queryBuilder.getMinimumShouldMatchField() != null
            || (queryBuilder.getMinimumShouldMatchScript() != null && queryBuilder.getValues().isEmpty())
            || queryBuilder.getMinimumShouldMatch() != null;
        SearchExecutionContext context = createSearchExecutionContext();
        rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertEquals(
            "query should " + (isCacheable ? "" : "not") + " be cacheable: " + queryBuilder.toString(),
            isCacheable,
            context.isCacheable()
        );

        // specifically trigger the two cases where query is cacheable
        queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, Collections.singletonList("foo"));
        queryBuilder.setMinimumShouldMatchField("m_s_m");
        context = createSearchExecutionContext();
        rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());

        queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, Collections.emptyList());
        queryBuilder.setMinimumShouldMatchScript(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap()));
        context = createSearchExecutionContext();
        rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());

        // also test one case where query is not cacheable
        queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, Collections.singletonList("foo"));
        queryBuilder.setMinimumShouldMatchScript(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap()));
        context = createSearchExecutionContext();
        rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertFalse("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    @Override
    public TermsSetQueryBuilder mutateInstance(final TermsSetQueryBuilder instance) throws IOException {
        String fieldName = instance.getFieldName();
        List<?> values = instance.getValues();
        String minimumShouldMatchField = null;
        Script minimumShouldMatchScript = null;
        String minimumShouldMatch = null;

        switch (randomIntBetween(0, 4)) {
            case 0 -> {
                Predicate<String> predicate = s -> s.equals(instance.getFieldName()) == false && s.equals(GEO_POINT_FIELD_NAME) == false;
                fieldName = randomValueOtherThanMany(predicate, () -> randomFrom(MAPPED_FIELD_NAMES));
                values = randomValues(fieldName);
            }
            case 1 -> values = randomValues(fieldName);
            case 2 -> minimumShouldMatchField = randomAlphaOfLengthBetween(1, 10);
            case 3 -> minimumShouldMatchScript = new Script(ScriptType.INLINE, MockScriptEngine.NAME, randomAlphaOfLength(10), emptyMap());
            case 4 -> minimumShouldMatch = "3";
        }

        TermsSetQueryBuilder newInstance = new TermsSetQueryBuilder(fieldName, values);
        if (minimumShouldMatchField != null) {
            newInstance.setMinimumShouldMatchField(minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            newInstance.setMinimumShouldMatchScript(minimumShouldMatchScript);
        }
        if (minimumShouldMatch != null) {
            newInstance.setMinimumShouldMatch(minimumShouldMatch);
        }
        return newInstance;
    }

    public void testBothFieldAndScriptSpecified() {
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder("_field", Collections.emptyList());
        queryBuilder.setMinimumShouldMatchScript(new Script(""));
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatchField("_field"));
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatch("2"));

        queryBuilder.setMinimumShouldMatchScript(null);
        queryBuilder.setMinimumShouldMatchField("_field");
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatchScript(new Script("")));
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatch("2"));

        queryBuilder.setMinimumShouldMatchField(null);
        queryBuilder.setMinimumShouldMatch("2");
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatchField("_field"));
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatchScript(new Script("")));

    }

    public void testDoToQuery() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter iw = new IndexWriter(directory, config)) {
                Document document = new Document();
                document.add(new TextField("message", "a b", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 1));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 1));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 2));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 1));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 2));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d f g", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 3));
                iw.addDocument(document);
            }

            try (IndexReader ir = DirectoryReader.open(directory)) {
                SearchExecutionContext context = createSearchExecutionContext();
                Query queryWithMinimumShouldMatchField = new TermsSetQueryBuilder("message", Arrays.asList("c", "d"))
                    .setMinimumShouldMatchField("m_s_m")
                    .doToQuery(context);
                IndexSearcher searcher = newSearcher(ir);
                TopDocs topDocsWithMinimumShouldMatchField = searcher.search(
                    queryWithMinimumShouldMatchField,
                    10,
                    new Sort(SortField.FIELD_DOC)
                );
                assertThat(topDocsWithMinimumShouldMatchField.totalHits.value, equalTo(3L));
                assertThat(topDocsWithMinimumShouldMatchField.scoreDocs[0].doc, equalTo(1));
                assertThat(topDocsWithMinimumShouldMatchField.scoreDocs[1].doc, equalTo(3));
                assertThat(topDocsWithMinimumShouldMatchField.scoreDocs[2].doc, equalTo(4));

                context = createSearchExecutionContext();
                Query queryWithMinimumShouldMatch = new TermsSetQueryBuilder("message", Arrays.asList("c", "d", "a")).setMinimumShouldMatch(
                    "2"
                ).doToQuery(context);
                searcher = newSearcher(ir);
                TopDocs topDocsWithMinimumShouldMatch = searcher.search(queryWithMinimumShouldMatch, 10, new Sort(SortField.FIELD_DOC));
                assertThat(topDocsWithMinimumShouldMatch.totalHits.value, equalTo(5L));
                assertThat(topDocsWithMinimumShouldMatch.scoreDocs[0].doc, equalTo(1));
                assertThat(topDocsWithMinimumShouldMatch.scoreDocs[1].doc, equalTo(2));
                assertThat(topDocsWithMinimumShouldMatch.scoreDocs[2].doc, equalTo(3));
                assertThat(topDocsWithMinimumShouldMatch.scoreDocs[3].doc, equalTo(4));
                assertThat(topDocsWithMinimumShouldMatch.scoreDocs[4].doc, equalTo(5));

                context = createSearchExecutionContext();
                Query queryWithMinimumShouldMatchNegative = new TermsSetQueryBuilder("message", Arrays.asList("c", "g", "f"))
                    .setMinimumShouldMatch("-1")
                    .doToQuery(context);
                searcher = newSearcher(ir);
                TopDocs topDocsWithMinimumShouldMatchNegative = searcher.search(
                    queryWithMinimumShouldMatchNegative,
                    10,
                    new Sort(SortField.FIELD_DOC)
                );
                assertThat(topDocsWithMinimumShouldMatchNegative.totalHits.value, equalTo(1L));
                assertThat(topDocsWithMinimumShouldMatchNegative.scoreDocs[0].doc, equalTo(5));
            }
        }
    }

    public void testDoToQuery_msmScriptField() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter iw = new IndexWriter(directory, config)) {
                Document document = new Document();
                document.add(new TextField("message", "a b x y", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 50));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b x y", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 75));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c x", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 75));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c x", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 100));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 100));
                iw.addDocument(document);
            }

            try (IndexReader ir = DirectoryReader.open(directory)) {
                SearchExecutionContext context = createSearchExecutionContext();
                Script script = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap());
                Query query = new TermsSetQueryBuilder("message", Arrays.asList("a", "b", "c", "d")).setMinimumShouldMatchScript(script)
                    .doToQuery(context);
                IndexSearcher searcher = newSearcher(ir);
                TopDocs topDocs = searcher.search(query, 10, new Sort(SortField.FIELD_DOC));
                assertThat(topDocs.totalHits.value, equalTo(3L));
                assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
                assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
                assertThat(topDocs.scoreDocs[2].doc, equalTo(4));
            }
        }
    }

    public void testFieldAlias() {
        List<String> randomTerms = Arrays.asList(generateRandomStringArray(5, 10, false, false));
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(TEXT_ALIAS_FIELD_NAME, randomTerms).setMinimumShouldMatchField(
            "m_s_m"
        );

        SearchExecutionContext context = createSearchExecutionContext();
        List<Query> termQueries = queryBuilder.createTermQueries(context);
        assertEquals(randomTerms.size(), termQueries.size());

        String expectedFieldName = expectedFieldName(queryBuilder.getFieldName());
        for (int i = 0; i < randomTerms.size(); i++) {
            Term term = new Term(expectedFieldName, randomTerms.get(i));
            assertThat(termQueries.get(i), equalTo(new TermQuery(term)));
        }
    }

    private static List<?> randomValues(final String fieldName) {
        final int numValues = randomIntBetween(0, 10);
        final List<Object> values = new ArrayList<>(numValues);

        for (int i = 0; i < numValues; i++) {
            values.add(getRandomValueForFieldName(fieldName));
        }
        return values;
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("_script", args -> {
                try {
                    int clauseCount = ObjectPath.evaluate(args, "params.num_terms");
                    long msm = ((ScriptDocValues.Longs) ObjectPath.evaluate(args, "doc.m_s_m")).getValue();
                    return clauseCount * (msm / 100d);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

}
