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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.search.BooleanClause.Occur.*;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class XBooleanFilterTests extends ElasticsearchLuceneTestCase {

    private Directory directory;
    private AtomicReader reader;
    private static final char[] distinctValues = new char[] {'a', 'b', 'c', 'd', 'v','z','y'};

    @Before
    public void setup() throws Exception {
        super.setUp();
        char[][] documentMatrix = new char[][] {
                {'a', 'b', 'c', 'd', 'v'},
                {'a', 'b', 'c', 'd', 'z'},
                {'a', 'a', 'a', 'a', 'x'}
        };

        List<Document> documents = new ArrayList<>(documentMatrix.length);
        for (char[] fields : documentMatrix) {
            Document document = new Document();
            for (int i = 0; i < fields.length; i++) {
                document.add(new StringField(Integer.toString(i), String.valueOf(fields[i]), Field.Store.NO));
            }
            documents.add(document);
        }
        directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, new IndexWriterConfig(Lucene.VERSION, new KeywordAnalyzer()));
        w.addDocuments(documents);
        w.close();
        reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(directory));
    }

    @After
    public void tearDown() throws Exception {
        reader.close();
        directory.close();
        super.tearDown();

    }

    @Test
    public void testWithTwoClausesOfEachOccur_allFixedBitsetFilters() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, false)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testWithTwoClausesOfEachOccur_allBitsBasedFilters() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, true)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testWithTwoClausesOfEachOccur_allFilterTypes() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, false)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }

        booleanFilters.clear();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, true)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testWithTwoClausesOfEachOccur_singleClauseOptimisation() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'b', MUST, true)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }

        booleanFilters.clear();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'c', MUST_NOT, true)
        ));
        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(3));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(true));
        }

        booleanFilters.clear();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, true)
        ));
        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testOnlyShouldClauses() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<>();
        // 2 slow filters
        // This case caused: https://github.com/elasticsearch/elasticsearch/issues/2826
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'a', SHOULD, true),
                newFilterClause(1, 'b', SHOULD, true)
        ));
        // 2 fast filters
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'a', SHOULD, false),
                newFilterClause(1, 'b', SHOULD, false)
        ));
        // 1 fast filters, 1 slow filter
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'a', SHOULD, true),
                newFilterClause(1, 'b', SHOULD, false)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(3));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(true));
        }
    }

    @Test
    public void testOnlyMustClauses() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<>();
        // Slow filters
        booleanFilters.add(createBooleanFilter(
                newFilterClause(3, 'd', MUST, true),
                newFilterClause(3, 'd', MUST, true)
        ));
        // 2 fast filters
        booleanFilters.add(createBooleanFilter(
                newFilterClause(3, 'd', MUST, false),
                newFilterClause(3, 'd', MUST, false)
        ));
        // 1 fast filters, 1 slow filter
        booleanFilters.add(createBooleanFilter(
                newFilterClause(3, 'd', MUST, true),
                newFilterClause(3, 'd', MUST, false)
        ));
        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testOnlyMustNotClauses() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<>();
        // Slow filters
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'a', MUST_NOT, true),
                newFilterClause(1, 'a', MUST_NOT, true)
        ));
        // 2 fast filters
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'a', MUST_NOT, false),
                newFilterClause(1, 'a', MUST_NOT, false)
        ));
        // 1 fast filters, 1 slow filter
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'a', MUST_NOT, true),
                newFilterClause(1, 'a', MUST_NOT, false)
        ));
        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testNonMatchingSlowShouldWithMatchingMust() throws Exception {
        XBooleanFilter booleanFilter = createBooleanFilter(
                newFilterClause(0, 'a', MUST, false),
                newFilterClause(0, 'b', SHOULD, true)
        );

        DocIdSet docIdSet = booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs());
        assertThat(docIdSet, equalTo(null));
    }

    @Test
    public void testSlowShouldClause_atLeastOneShouldMustMatch() throws Exception {
        XBooleanFilter booleanFilter = createBooleanFilter(
                newFilterClause(0, 'a', MUST, false),
                newFilterClause(1, 'a', SHOULD, true)
        );

        FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(1));
        assertThat(result.get(0), equalTo(false));
        assertThat(result.get(1), equalTo(false));
        assertThat(result.get(2), equalTo(true));

        booleanFilter = createBooleanFilter(
                newFilterClause(0, 'a', MUST, false),
                newFilterClause(1, 'a', SHOULD, true),
                newFilterClause(4, 'z', SHOULD, true)
        );

        result = new FixedBitSet(reader.maxDoc());
        result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(2));
        assertThat(result.get(0), equalTo(false));
        assertThat(result.get(1), equalTo(true));
        assertThat(result.get(2), equalTo(true));
    }

    @Test
    // See issue: https://github.com/elasticsearch/elasticsearch/issues/4130
    public void testOneFastMustNotOneFastShouldAndOneSlowShould() throws Exception {
        XBooleanFilter booleanFilter = createBooleanFilter(
                newFilterClause(4, 'v', MUST_NOT, false),
                newFilterClause(4, 'z', SHOULD, false),
                newFilterClause(4, 'x', SHOULD, true)
        );

        FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(2));
        assertThat(result.get(0), equalTo(false));
        assertThat(result.get(1), equalTo(true));
        assertThat(result.get(2), equalTo(true));
    }

    @Test
    public void testOneFastShouldClauseAndOneSlowShouldClause() throws Exception {
        XBooleanFilter booleanFilter = createBooleanFilter(
                newFilterClause(4, 'z', SHOULD, false),
                newFilterClause(4, 'x', SHOULD, true)
        );

        FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(2));
        assertThat(result.get(0), equalTo(false));
        assertThat(result.get(1), equalTo(true));
        assertThat(result.get(2), equalTo(true));
    }

    @Test
    public void testOneMustClauseOneFastShouldClauseAndOneSlowShouldClause() throws Exception {
        XBooleanFilter booleanFilter = createBooleanFilter(
                newFilterClause(0, 'a', MUST, false),
                newFilterClause(4, 'z', SHOULD, false),
                newFilterClause(4, 'x', SHOULD, true)
        );

        FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(2));
        assertThat(result.get(0), equalTo(false));
        assertThat(result.get(1), equalTo(true));
        assertThat(result.get(2), equalTo(true));
    }

    private static FilterClause newFilterClause(int field, char character, BooleanClause.Occur occur, boolean slowerBitsBackedFilter) {
        Filter filter;
        if (slowerBitsBackedFilter) {
            filter = new PrettyPrintFieldCacheTermsFilter(String.valueOf(field), String.valueOf(character));
        } else {
            Term term = new Term(String.valueOf(field), String.valueOf(character));
            filter = new TermFilter(term);
        }
        return new FilterClause(filter, occur);
    }

    private static XBooleanFilter createBooleanFilter(FilterClause... clauses) {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        for (FilterClause clause : clauses) {
            booleanFilter.add(clause);
        }
        return booleanFilter;
    }

    @Test
    public void testRandom() throws IOException {
        int iterations = scaledRandomIntBetween(100, 1000); // don't worry that is fast!
        for (int iter = 0; iter < iterations; iter++) {
            int numClauses = 1 + random().nextInt(10);
            FilterClause[] clauses = new FilterClause[numClauses];
            BooleanQuery topLevel = new BooleanQuery();
            BooleanQuery orQuery = new BooleanQuery();
            boolean hasMust = false;
            boolean hasShould = false;
            boolean hasMustNot = false;
            for(int i = 0; i < numClauses; i++) {
                int field = random().nextInt(5);
                char value = distinctValues[random().nextInt(distinctValues.length)];
                switch(random().nextInt(10)) {
                    case 9:
                    case 8:
                    case 7:
                    case 6:
                    case 5:
                        hasMust = true;
                        if (rarely()) {
                            clauses[i] = new FilterClause(new EmptyFilter(), MUST);
                            topLevel.add(new BooleanClause(new MatchNoDocsQuery(), MUST));
                        } else {
                            clauses[i] = newFilterClause(field, value, MUST, random().nextBoolean());
                            topLevel.add(new BooleanClause(new TermQuery(new Term(String.valueOf(field), String.valueOf(value))), MUST));
                        }
                        break;
                    case 4:
                    case 3:
                    case 2:
                    case 1:
                        hasShould = true;
                        if (rarely()) {
                            clauses[i] = new FilterClause(new EmptyFilter(), SHOULD);
                            orQuery.add(new BooleanClause(new MatchNoDocsQuery(), SHOULD));
                        } else {
                            clauses[i] = newFilterClause(field, value, SHOULD, random().nextBoolean());
                            orQuery.add(new BooleanClause(new TermQuery(new Term(String.valueOf(field), String.valueOf(value))), SHOULD));
                        }
                        break;
                    case 0:
                        hasMustNot = true;
                        if (rarely()) {
                            clauses[i] = new FilterClause(new EmptyFilter(), MUST_NOT);
                            topLevel.add(new BooleanClause(new MatchNoDocsQuery(), MUST_NOT));
                        } else {
                            clauses[i] = newFilterClause(field, value, MUST_NOT, random().nextBoolean());
                            topLevel.add(new BooleanClause(new TermQuery(new Term(String.valueOf(field), String.valueOf(value))), MUST_NOT));
                        }
                        break;

                }
            }
            if (orQuery.getClauses().length > 0) {
                topLevel.add(new BooleanClause(orQuery, MUST));
            }
            if (hasMustNot && !hasMust && !hasShould) {  // pure negative
                topLevel.add(new BooleanClause(new MatchAllDocsQuery(), MUST));
            }
            XBooleanFilter booleanFilter = createBooleanFilter(clauses);

            FixedBitSet leftResult = new FixedBitSet(reader.maxDoc());
            FixedBitSet rightResult = new FixedBitSet(reader.maxDoc());
            DocIdSet left = booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs());
            DocIdSet right = new QueryWrapperFilter(topLevel).getDocIdSet(reader.getContext(), reader.getLiveDocs());
            if (left == null || right == null) {
                if (left == null && right != null) {
                    assertThat(errorMsg(clauses, topLevel), (right.iterator() == null ? DocIdSetIterator.NO_MORE_DOCS : right.iterator().nextDoc()), equalTo(DocIdSetIterator.NO_MORE_DOCS));
                }
                if (left != null && right == null) {
                    assertThat(errorMsg(clauses, topLevel), (left.iterator() == null ? DocIdSetIterator.NO_MORE_DOCS : left.iterator().nextDoc()), equalTo(DocIdSetIterator.NO_MORE_DOCS));
                }
            } else {
                DocIdSetIterator leftIter = left.iterator();
                DocIdSetIterator rightIter = right.iterator();
                if (leftIter != null) {
                    leftResult.or(leftIter);
                }

                if (rightIter != null) {
                    rightResult.or(rightIter);
                }

                assertThat(leftResult.cardinality(), equalTo(rightResult.cardinality()));
                for (int i = 0; i < reader.maxDoc(); i++) {
                    assertThat(errorMsg(clauses, topLevel) + " -- failed at index " + i, leftResult.get(i), equalTo(rightResult.get(i)));
                }
            }
        }
    }

    private String errorMsg(FilterClause[] clauses, BooleanQuery query) {
        return query.toString() + " vs. " + Arrays.toString(clauses);
    }


    public static final class PrettyPrintFieldCacheTermsFilter extends FieldCacheTermsFilter {

        private final String value;
        private final String field;

        public PrettyPrintFieldCacheTermsFilter(String field, String value) {
            super(field, value);
            this.field = field;
            this.value = value;
        }

        @Override
        public String toString() {
            return "SLOW(" + field + ":" + value + ")";
        }
    }

    public final class EmptyFilter extends Filter {

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            return random().nextBoolean() ? new Empty() : null;
        }

        private class Empty extends DocIdSet {

            @Override
            public DocIdSetIterator iterator() throws IOException {
                return null;
            }
        }
    }

}

