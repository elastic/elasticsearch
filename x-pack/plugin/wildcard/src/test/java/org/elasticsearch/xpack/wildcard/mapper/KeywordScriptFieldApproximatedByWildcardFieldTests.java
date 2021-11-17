/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.KeywordScriptFieldType;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.StringFieldScript.LeafFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.AbstractScriptFieldQuery;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class KeywordScriptFieldApproximatedByWildcardFieldTests extends MapperServiceTestCase {
    public void testTermQueryApproximated() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> b.startObject("foo").field("type", "wildcard").endObject()));
        WildcardFieldMapper mapper = (WildcardFieldMapper) documentMapper.mappers().getMapper("foo");
        try (
            Directory directory = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory, WildcardFieldMapper.WILDCARD_ANALYZER_7_10)
        ) {
            index(iw, mapper, "foo bar");
            index(iw, mapper, "baz bort");
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, mapper.fieldType());
                MappedFieldType ft = build("foo_approximated", Map.of());
                assertCountAndApproximationTerms(
                    searcher,
                    ft.termQuery("foo bar", context),
                    equalTo(1),
                    "[0 65 6f]",
                    "eoo",
                    "oo/",
                    "o/a",
                    "/aa",
                    "aaq",
                    "[61 71 0]",
                    "[71 0 0]"
                );
                assertCountAndApproximationTerms(
                    searcher,
                    ft.termQuery("baz bort", context),
                    equalTo(1),
                    "[0 61 61]",
                    "aay",
                    "ay/",
                    "y/a",
                    "/ao",
                    "aoq",
                    "oqs",
                    "[71 73 0]",
                    "[73 0 0]"
                );
                assertCountAndApproximationTerms(searcher, ft.termQuery("a", context), equalTo(0), "[0 61 0]", "[61 0 0]");
                assertCountAndApproximationTerms(
                    searcher,
                    ft.termQuery("foo", context),
                    equalTo(0),
                    "[0 65 6f]",
                    "eoo",
                    "[6f 6f 0]",
                    "[6f 0 0]"
                );

                ft = build("foo_last_word_approximated", Map.of());
                assertCountAndApproximationTerms(searcher, ft.termQuery("bar", context), equalTo(1), "aaq");
                assertCountAndApproximationTerms(searcher, ft.termQuery("bort", context), equalTo(1), "aoq", "oqs");
                assertCountAndApproximationTerms(searcher, ft.termQuery("bor", context), equalTo(0), "aoq");
                assertCountAndApproximation(searcher, ft.termQuery("f", context), equalTo(0), new MatchAllDocsQuery());
                assertCountAndApproximation(searcher, ft.termQuery("fo", context), equalTo(0), new MatchAllDocsQuery());
            }
        }
    }

    private void index(RandomIndexWriter iw, WildcardFieldMapper mapper, String value) throws IOException {
        List<IndexableField> fields = new ArrayList<>();
        LuceneDocument parseDoc = new LuceneDocument();
        mapper.createFields(value, parseDoc, fields);
        for (IndexableField field : fields) {
            parseDoc.add(field);
        }
        iw.addDocument(parseDoc.getFields());
    }

    private static void assertCountAndApproximationTerms(
        IndexSearcher searcher,
        Query query,
        Matcher<Integer> count,
        String... expectedApproximationTerms
    ) throws IOException {
        assertThat(searcher.count(query), count);
        assertThat(query, instanceOf(AbstractScriptFieldQuery.class));
        Query approximation = ((AbstractScriptFieldQuery<?>) searcher.rewrite(query)).approximation();
        assertThat(approximationTermFor(approximation), containsInAnyOrder(expectedApproximationTerms));
    }

    private static void assertCountAndApproximation(
        IndexSearcher searcher,
        Query query,
        Matcher<Integer> count,
        Query expectedApproximation
    ) throws IOException {
        assertThat(searcher.count(query), count);
        assertThat(query, instanceOf(AbstractScriptFieldQuery.class));
        Query approximation = ((AbstractScriptFieldQuery<?>) searcher.rewrite(query)).approximation();
        assertThat(approximation, equalTo(expectedApproximation));
    }

    private static List<String> approximationTermFor(Query approximation) {
        if (approximation instanceof TermQuery) {
            return List.of(approximateTermToString(((TermQuery) approximation).getTerm()));
        }
        assertThat(approximation, instanceOf(BooleanQuery.class));
        List<String> approximationTerms = new ArrayList<>();
        BooleanQuery bq = (BooleanQuery) approximation;
        for (BooleanClause clause : bq.clauses()) {
            assertThat(clause.getOccur(), equalTo(Occur.MUST));
            assertThat(clause.getQuery(), instanceOf(TermQuery.class));
            Term term = ((TermQuery) clause.getQuery()).getTerm();
            String text = term.text();
            if (text.contains(Character.toString(WildcardFieldMapper.TOKEN_START_OR_END_CHAR))) {
                text = term.bytes().toString();
            }
            approximationTerms.add(text);
        }
        return approximationTerms;
    }

    private static String approximateTermToString(Term term) {
        String text = term.text();
        if (text.contains(Character.toString(WildcardFieldMapper.TOKEN_START_OR_END_CHAR))) {
            return term.bytes().toString();
        }
        return text;
    }

    private static KeywordScriptFieldType build(String code, Map<String, Object> params) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new KeywordScriptFieldType("test", factory(script), script, true, emptyMap());
    }

    private static StringFieldScript.Factory factory(Script script) {
        switch (script.getIdOrCode()) {
            case "foo_approximated":
                return new StringFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return ctx -> new StringFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (String foo : (ScriptDocValues.Strings) getDoc().get("foo")) {
                                    emit(foo);
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpressionBuilder emitExpression() {
                        return QueryableExpressionBuilder.field("foo");
                    }
                };
            case "foo_last_word_approximated":
                return new StringFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return ctx -> new StringFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (String foo : (ScriptDocValues.Strings) getDoc().get("foo")) {
                                    emit(foo.substring(foo.indexOf(' ') + 1));
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpressionBuilder emitExpression() {
                        return QueryableExpressionBuilder.substring(QueryableExpressionBuilder.field("foo"));
                    }
                };
            default:
                throw new IllegalArgumentException("unsupported script [" + script.getIdOrCode() + "]");
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new Wildcard());
    }

}
