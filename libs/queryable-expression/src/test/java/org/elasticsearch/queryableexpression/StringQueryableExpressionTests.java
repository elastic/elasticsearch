/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.queryableexpression.LongQueryableExpressionTests.assertCount;
import static org.hamcrest.Matchers.equalTo;

public class StringQueryableExpressionTests extends ESTestCase {
    public void testStringFieldAlone() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.field("foo");
        withIndexedKeyword((indexed, searcher, foo) -> {
            StringQueryableExpression expression = builder.build(f -> foo, null).castToString();
            assertThat(expression.toString(), equalTo("foo"));
            checkApproximations(searcher, expression, indexed);
            checkPerfectApproximation(searcher, expression, indexed);
        });
    }

    public void testLongFieldAppliedToUnknownOp() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.unknownOp(QueryableExpressionBuilder.field("foo"));

        withIndexedKeyword((indexed, searcher, foo) -> {
            StringQueryableExpression expression = builder.build(f -> foo, null).castToString();
            assertThat(expression.toString(), equalTo("unknown(foo)"));
            checkApproximations(searcher, expression, indexed);
        });
    }

    public void testMissingLongFieldAppliedToUnknownOp() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.unknownOp(QueryableExpressionBuilder.field("foo"));

        withKeywordField(iw -> iw.addDocument(List.of()), (searcher, foo) -> {
            StringQueryableExpression expression = builder.build(f -> foo, null).castToString();
            assertThat(expression.toString(), equalTo("unknown(foo)"));
            assertCount(searcher, expression.approximateTermQuery(randomInterestingString()), 0);
        });
    }

    public void testStringFieldSubstring() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.substring(QueryableExpressionBuilder.field("foo"));
        withIndexedKeyword((indexed, searcher, foo) -> {
            StringQueryableExpression expression = builder.build(f -> foo, null).castToString();
            assertThat(expression.toString(), equalTo("foo.substring()"));
            checkApproximations(searcher, expression, indexed);
            String substring = randomSubstring(indexed);
            checkApproximations(searcher, expression, substring);
            String nonSubstring = randomValueOtherThanMany(indexed::contains, StringQueryableExpressionTests::randomInterestingString);
            assertCount(searcher, expression.approximateTermQuery(nonSubstring), 0);
            String invalidUtf16 = new String(new char[] { (char) between(Character.MIN_SURROGATE, Character.MAX_SURROGATE) });
            assertCount(searcher, expression.approximateTermQuery(invalidUtf16), 1);
        });
    }

    private String randomSubstring(String str) {
        int substringStart = between(0, str.length());
        int substringEnd = between(substringStart, str.length());
        return str.substring(substringStart, substringEnd);
    }

    private void checkApproximations(IndexSearcher searcher, StringQueryableExpression expression, String result) throws IOException {
        assertCount(searcher, expression.approximateTermQuery(result), 1);
    }

    private void checkPerfectApproximation(IndexSearcher searcher, StringQueryableExpression expression, String result) throws IOException {
        assertCount(
            searcher,
            expression.approximateTermQuery(randomValueOtherThan(result, StringQueryableExpressionTests::randomInterestingString)),
            0
        );
    }

    static String randomInterestingString() {
        switch (between(0, 4)) {
            case 0:
                return "";
            case 1:
                return randomAlphaOfLengthBetween(1, 10);
            case 2:
                return randomRealisticUnicodeOfCodepointLengthBetween(1, 10);
            case 3:
                return randomAlphaOfLengthBetween(10, 1000);
            case 4:
                return randomRealisticUnicodeOfCodepointLengthBetween(10, 1000);
            default:
                throw new IllegalArgumentException("Unsupported case");
        }
    }

    @FunctionalInterface
    interface WithIndexedKeyword {
        void accept(String indexed, IndexSearcher searcher, QueryableExpression foo) throws IOException;
    }

    private static void withIndexedKeyword(WithIndexedKeyword callback) throws IOException {
        String indexed = randomInterestingString();
        withKeywordField(
            iw -> iw.addDocument(List.of(new Field("foo", indexed, FIELD_TYPE), new SortedSetDocValuesField("foo", new BytesRef(indexed)))),
            (searcher, foo) -> callback.accept(indexed, searcher, foo)
        );
    }

    private static void withKeywordField(
        CheckedConsumer<RandomIndexWriter, IOException> builder,
        CheckedBiConsumer<IndexSearcher, StringQueryableExpression, IOException> callback
    ) throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            builder.accept(iw);
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                callback.accept(searcher, StringQueryableExpression.field("foo", new StringQueryableExpression.Queries() {
                    @Override
                    public Query approximateExists() {
                        return new DocValuesFieldExistsQuery("foo");
                    }

                    @Override
                    public Query approximateTermQuery(String term) {
                        return new TermQuery(new Term("foo", term));
                    }

                    @Override
                    public Query approximateSubstringQuery(String term) {
                        if (false == UnicodeUtil.validUTF16String(term)) {
                            return new MatchAllDocsQuery();
                        }
                        return new AutomatonQuery(
                            new Term("foo", "*" + term + "*"),
                            Operations.concatenate(List.of(Automata.makeAnyString(), Automata.makeString(term), Automata.makeAnyString()))
                        );
                    }
                }));
            }
        }
    }

    static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.freeze();
    }
}
