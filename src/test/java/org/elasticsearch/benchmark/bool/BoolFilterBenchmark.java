/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.benchmark.bool;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilterTests;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.unit.SizeValue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.MUST_NOT;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Run with the following jvm args: -Xms2G -Xmx2G -server
 */
public class BoolFilterBenchmark {

    static final Random random = new Random();
    static final long numUniqueValues = SizeValue.parseSizeValue("50k").singles();
    static final long numDocs = SizeValue.parseSizeValue("1m").singles();
    static final long numQueries = SizeValue.parseSizeValue("20k").singles();
    static final long warmUp = SizeValue.parseSizeValue("10k").singles();

    public static void main(String[] args) throws Exception {
        FSDirectory dir = FSDirectory.open(new File("work/test"));
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        final IndexReader reader;
        final String[] uniqueValuesLookup;
        if (writer.numDocs() != numDocs) {
            ObjectOpenHashSet<String> uniqueValues = new ObjectOpenHashSet<String>();
            while (uniqueValues.size() != numUniqueValues) {
                uniqueValues.add(Strings.randomBase64UUID());
            }
            uniqueValuesLookup = uniqueValues.toArray(String.class);

            StopWatch watch = new StopWatch().start();
            System.out.println("Indexing " + numDocs + " docs with " + numUniqueValues + " unique values...");
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("0", uniqueValuesLookup[random.nextInt(uniqueValuesLookup.length)], Field.Store.NO));
                doc.add(new StringField("1", uniqueValuesLookup[random.nextInt(uniqueValuesLookup.length)], Field.Store.NO));
                doc.add(new StringField("2", uniqueValuesLookup[random.nextInt(uniqueValuesLookup.length)], Field.Store.NO));
                doc.add(new StringField("3", uniqueValuesLookup[random.nextInt(uniqueValuesLookup.length)], Field.Store.NO));
                writer.addDocument(doc);
                if (random.nextInt(1000) == 456) {
                    writer.commit();
                }
            }
            System.out.println("Done indexing, took " + watch.stop().lastTaskTime());
            reader = DirectoryReader.open(writer, true);
        } else {
            System.out.println("Using existing index");
            ObjectOpenHashSet<String> uniqueValues = new ObjectOpenHashSet<String>();
            reader = DirectoryReader.open(writer, true);
            AtomicReader slowReader = SlowCompositeReaderWrapper.wrap(reader);
            for (String field : slowReader.fields()) {
                Terms terms = slowReader.terms(field);
                TermsEnum termsEnum = terms.iterator(null);
                for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                    uniqueValues.add(term.utf8ToString());
                }
            }

            if (uniqueValues.size() != numUniqueValues) {
                System.err.println("Not all unique values have been loaded");
            }
            uniqueValuesLookup = uniqueValues.toArray(String.class);
        }

        IndexSearcher searcher = new IndexSearcher(reader);
        warmUp(searcher, uniqueValuesLookup);

        checkPerformance("Only must fast", searcher, uniqueValuesLookup, ExecutionType.FAST, FilterType.ONLY_MUST);
        checkPerformance("Only must slow", searcher, uniqueValuesLookup, ExecutionType.SLOW, FilterType.ONLY_MUST);
        checkPerformance("Only must random", searcher, uniqueValuesLookup, ExecutionType.RANDOM, FilterType.ONLY_MUST);

        checkPerformance("must, must_not fast", searcher, uniqueValuesLookup, ExecutionType.FAST, FilterType.MUST_MOST_NOT);
        checkPerformance("must, must_not slow", searcher, uniqueValuesLookup, ExecutionType.SLOW, FilterType.MUST_MOST_NOT);
        checkPerformance("must, must_not random", searcher, uniqueValuesLookup, ExecutionType.RANDOM, FilterType.MUST_MOST_NOT);

        checkPerformance("Only should fast", searcher, uniqueValuesLookup, ExecutionType.FAST, FilterType.ONLY_SHOULD);
        checkPerformance("Only should slow", searcher, uniqueValuesLookup, ExecutionType.SLOW, FilterType.ONLY_SHOULD);
        checkPerformance("Only should random slow", searcher, uniqueValuesLookup, ExecutionType.RANDOM, FilterType.ONLY_SHOULD);

        checkPerformance("Must,must_not,should fast", searcher, uniqueValuesLookup, ExecutionType.FAST, FilterType.MUST_MUST_NOT_SHOULD);
        checkPerformance("Must,must_not,should slow", searcher, uniqueValuesLookup, ExecutionType.SLOW, FilterType.MUST_MUST_NOT_SHOULD);
        checkPerformance("Must,must_not,should random slow", searcher, uniqueValuesLookup, ExecutionType.RANDOM, FilterType.MUST_MUST_NOT_SHOULD);

        reader.close();
        writer.close();
    }

    public static void checkPerformance(String label, IndexSearcher searcher, String[] uniqueValuesLookup, ExecutionType executionType, FilterType filterType) throws IOException {
        int maxNumClauses = 3;
        long totalTime = 0;
        int numErrors = 0;
        for (int i = 0; i < numQueries; i++) {
            XBooleanFilter booleanFilter = filterType.filter(maxNumClauses, executionType, uniqueValuesLookup);
            booleanFilter.setMinimumShouldMatch(1);

            XConstantScoreQuery query = new XConstantScoreQuery(booleanFilter);
            long startTime = System.nanoTime();
            TotalHitCountCollector hitCountCollector = new TotalHitCountCollector();
            searcher.search(query, hitCountCollector);
            long result = hitCountCollector.getTotalHits();
            long timeTook = System.nanoTime() - startTime;
            totalTime += timeTook;

            BooleanQuery booleanQuery = new BooleanQuery();
            boolean hasShould = false;
            boolean hasMust = false;
            boolean hasMustNot = false;
            for (FilterClause filterClause : booleanFilter) {
                BooleanClause.Occur occur = filterClause.getOccur();
                if (occur == SHOULD) {
                    hasShould = true;
                } else if (occur == MUST) {
                    hasMust = true;
                } else if (occur == MUST_NOT) {
                    hasMustNot = true;
                }
                booleanQuery.add(new XConstantScoreQuery(filterClause.getFilter()), filterClause.getOccur());
            }
            if (hasShould) {
                booleanQuery.setMinimumNumberShouldMatch(1);
            }
            if (hasMustNot && !hasMust && !hasShould) {  // pure negative
                booleanQuery.add(new BooleanClause(new MatchAllDocsQuery(), MUST));
            }

            searcher.search(booleanQuery, hitCountCollector = new TotalHitCountCollector());
            long control = hitCountCollector.getTotalHits();
            if (result != control) {
                numErrors++;
            }
        }

        if (numErrors > 0) {
            System.err.printf("Result and control were not equal %d times.\n", numErrors);
        }

        double totalTimeInS = totalTime / (1000 * 1000 * 1000);
        System.out.printf("[%s] Total time %f s | avg time per query %f s\n", label, totalTimeInS, (totalTimeInS / numQueries));
    }

    public static void warmUp(IndexSearcher searcher, String[] uniqueValuesLookup) throws IOException {
        int maxNumClauses = 3;
        for (ExecutionType executionType : ExecutionType.values()) {
            for (FilterType filterType : FilterType.values()) {
                for (int i = 0; i < warmUp; i++) {
                    XBooleanFilter booleanFilter = filterType.filter(maxNumClauses, executionType, uniqueValuesLookup);
                    booleanFilter.setMinimumShouldMatch(1);

                    XConstantScoreQuery query = new XConstantScoreQuery(booleanFilter);
                    TotalHitCountCollector hitCountCollector = new TotalHitCountCollector();
                    searcher.search(query, hitCountCollector);
                }
            }
        }
    }

    enum ExecutionType {

        FAST(),
        SLOW(),
        RANDOM();

        public boolean fast() {
            if (this == FAST) {
                return true;
            } else if (this == SLOW) {
                return false;
            } else {
                return random.nextInt(3) == 2;
            }
        }

    }

    enum FilterType {

        ONLY_SHOULD {
            @Override
            public XBooleanFilter filter(int maxNumClauses, ExecutionType executionType, String[] uniqueValuesLookup) {
                return createFilter(maxNumClauses, executionType, uniqueValuesLookup, BooleanClause.Occur.SHOULD);
            }
        },
        MUST_MUST_NOT_SHOULD {
            @Override
            public XBooleanFilter filter(int maxNumClauses, ExecutionType executionType, String[] uniqueValuesLookup) {
                return createFilter(maxNumClauses, executionType, uniqueValuesLookup, BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT, BooleanClause.Occur.SHOULD);
            }
        },
        MUST_MOST_NOT {
            @Override
            public XBooleanFilter filter(int maxNumClauses, ExecutionType executionType, String[] uniqueValuesLookup) {
                return createFilter(maxNumClauses, executionType, uniqueValuesLookup, BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT);
            }
        },
        ONLY_MUST {
            @Override
            public XBooleanFilter filter(int maxNumClauses, ExecutionType executionType, String[] uniqueValuesLookup) {
                return createFilter(maxNumClauses, executionType, uniqueValuesLookup, BooleanClause.Occur.MUST);
            }
        },
        ONLY_MUST_NOT {
            @Override
            public XBooleanFilter filter(int maxNumClauses, ExecutionType executionType, String[] uniqueValuesLookup) {
                return createFilter(maxNumClauses, executionType, uniqueValuesLookup, BooleanClause.Occur.MUST_NOT);
            }
        };

        public abstract XBooleanFilter filter(int maxNumClauses, ExecutionType executionType, String[] uniqueValuesLookup);

        private static XBooleanFilter createFilter(int maxNumClauses, ExecutionType executionType, String[] uniqueValuesLookup, BooleanClause.Occur... occurs) {
            XBooleanFilter booleanFilter = new XBooleanFilter();
            List<BooleanClause.Occur> suffledOccurs = Arrays.asList(occurs);
            Collections.shuffle(suffledOccurs);
            int numClauses = 1 + random.nextInt(maxNumClauses);
            for (int i = 0; i < numClauses; i++) {
                String field = Integer.toString(i);
                String randomValue = uniqueValuesLookup[random.nextInt(uniqueValuesLookup.length)];

                final Filter filter;
                if (executionType.fast()) {
                    // TermsFilter always result into FBS, TermFilter not.
                    filter = new TermsFilter(new Term(field, randomValue));
                } else {
                    filter = new XBooleanFilterTests.PrettyPrintFieldCacheTermsFilter(field, randomValue);
                }
                booleanFilter.add(filter, suffledOccurs.get(i % suffledOccurs.size()));
            }
            return booleanFilter;
        }

    }

}
