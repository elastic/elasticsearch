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
package org.elasticsearch.index.query;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.CoveringQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TermsSetQueryBuilderTests extends AbstractQueryTestCase<TermsSetQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        String docType = "doc";
        mapperService.merge(docType, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(docType,
                "m_s_m", "type=long"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    @Override
    protected TermsSetQueryBuilder doCreateTestQueryBuilder() {
        String fieldName;
        do {
            fieldName = randomFrom(MAPPED_FIELD_NAMES);
        } while (fieldName.equals(GEO_POINT_FIELD_NAME) || fieldName.equals(GEO_SHAPE_FIELD_NAME));
        int numValues = randomIntBetween(0, 10);
        List<Object> randomTerms = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            randomTerms.add(getRandomValueForFieldName(fieldName));
        }
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(STRING_FIELD_NAME, randomTerms);
        if (randomBoolean()) {
            queryBuilder.setMinimumShouldMatchField("m_s_m");
        } else {
            queryBuilder.setMinimumShouldMatchScript(
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", Collections.emptyMap()));
        }
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(TermsSetQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        if (queryBuilder.getValues().isEmpty()) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
            MatchNoDocsQuery matchNoDocsQuery = (MatchNoDocsQuery) query;
            assertThat(matchNoDocsQuery.toString(), containsString("No terms supplied for \"terms_set\" query."));
        } else {
            assertThat(query, instanceOf(CoveringQuery.class));
        }
    }

    @Override
    protected boolean isCachable(TermsSetQueryBuilder queryBuilder) {
        return queryBuilder.getMinimumShouldMatchField() != null ||
                (queryBuilder.getMinimumShouldMatchScript() != null && queryBuilder.getValues().isEmpty());
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }

    public void testBothFieldAndScriptSpecified() {
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder("_field", Collections.emptyList());
        queryBuilder.setMinimumShouldMatchScript(new Script(""));
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatchField("_field"));

        queryBuilder.setMinimumShouldMatchScript(null);
        queryBuilder.setMinimumShouldMatchField("_field");
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
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 3));
                iw.addDocument(document);
            }

            try (IndexReader ir = DirectoryReader.open(directory)) {
                QueryShardContext context = createShardContext();
                Query query = new TermsSetQueryBuilder("message", Arrays.asList("c", "d"))
                        .setMinimumShouldMatchField("m_s_m").doToQuery(context);
                IndexSearcher searcher = new IndexSearcher(ir);
                TopDocs topDocs = searcher.search(query, 10, new Sort(SortField.FIELD_DOC));
                assertThat(topDocs.totalHits, equalTo(3L));
                assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
                assertThat(topDocs.scoreDocs[1].doc, equalTo(3));
                assertThat(topDocs.scoreDocs[2].doc, equalTo(4));
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
                QueryShardContext context = createShardContext();
                Script script = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", Collections.emptyMap());
                Query query = new TermsSetQueryBuilder("message", Arrays.asList("a", "b", "c", "d"))
                        .setMinimumShouldMatchScript(script).doToQuery(context);
                IndexSearcher searcher = new IndexSearcher(ir);
                TopDocs topDocs = searcher.search(query, 10, new Sort(SortField.FIELD_DOC));
                assertThat(topDocs.totalHits, equalTo(3L));
                assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
                assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
                assertThat(topDocs.scoreDocs[2].doc, equalTo(4));
            }
        }
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

