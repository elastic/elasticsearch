/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
<<<<<<< HEAD

=======
>>>>>>> nik9000/esql_min_competitive_2
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class EsMinCompetitiveQueriesTests extends ESTestCase {
    private static final int NON_NULL_DOCS = 10_000;
    private static final int NULL_DOCS = 5;

    @ParametersFactory(argumentFormatting = "asc=%s nullsFirst=%s moreThanOneSortKey=%s fieldExists=%s")
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (boolean asc : new boolean[] { false, true }) {
            for (boolean nullsFirst : new boolean[] { false, true }) {
                for (boolean moreThanOneSortKey : new boolean[] { false, true }) {
                    for (boolean fieldExists : new boolean[] { false, true }) {
                        params.add(new Object[] { asc, nullsFirst, moreThanOneSortKey, fieldExists });
                    }
                }
            }
        }
        return params;
    }

    private final boolean asc;
    private final boolean nullsFirst;
    private final boolean moreThanOneSortKey;
    private final boolean fieldExists;

    private final Directory directory = newDirectory();
    private IndexReader reader;
    private IndexSearcher searcher;

    public EsMinCompetitiveQueriesTests(boolean asc, boolean nullsFirst, boolean moreThanOneSortKey, boolean fieldExists) {
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.moreThanOneSortKey = moreThanOneSortKey;
        this.fieldExists = fieldExists;
    }

    public void testNoData() throws IOException {
        assertThat(searcher.count(minCompetitiveQuery(null)), equalTo(NON_NULL_DOCS + NULL_DOCS));
    }

    public void testNullValue() throws IOException {
        Page page = new Page(TestBlockFactory.getNonBreakingInstance().newConstantNullBlock(1));
        assertThat(searcher.count(minCompetitiveQuery(page)), equalTo(expectedForNullKey()));
    }

    private int expectedForNullKey() {
        if (nullsFirst) {
            if (moreThanOneSortKey) {
                if (fieldExists) {
                    // Any doc with null may be higher
                    return NULL_DOCS;
                }
                return NON_NULL_DOCS + NULL_DOCS;
            }
            // We have enough docs, don't bother looking
            return 0;
        }
        if (moreThanOneSortKey) {
            // Any null *or* non-null document may be higher. That's everything.
            return NULL_DOCS + NON_NULL_DOCS;
        }
        if (fieldExists) {
            // Any non-null doc is better than what we have
            return NON_NULL_DOCS;
        }
        return 0;
    }

    public void testNonNullValue() throws IOException {
        long v = randomLongBetween(0, NON_NULL_DOCS);
        Page page = new Page(TestBlockFactory.getNonBreakingInstance().newConstantLongBlockWith(v, 1));
        assertThat(searcher.count(minCompetitiveQuery(page)), equalTo(expectedForKey(v)));
    }

    private int expectedForKey(long v) {
        if (fieldExists == false) {
            if (nullsFirst) {
                return NULL_DOCS + NON_NULL_DOCS;
            }
            return 0;
        }
        int expected = (int) (asc ? NON_NULL_DOCS - v - 1 : v);
        if (nullsFirst) {
            expected += NULL_DOCS;
        }
        if (moreThanOneSortKey) {
            expected += 1;
        }
        return expected;
    }

    private Query minCompetitiveQuery(Page minCompetitive) throws IOException {
        EsMinCompetitiveQueries queries = new EsMinCompetitiveQueries(setup(), ctx());
        return queries.buildMinCompetitiveQuery(minCompetitive);
    }

    private EsQueryExec.MinCompetitiveSetup setup() {
        List<SharedMinCompetitive.KeyConfig> keys = new ArrayList<>();
        keys.add(new SharedMinCompetitive.KeyConfig(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE, asc, nullsFirst));
        if (moreThanOneSortKey) {
            keys.add(
                new SharedMinCompetitive.KeyConfig(
                    randomFrom(ElementType.values()),
                    TopNEncoder.DEFAULT_SORTABLE,
                    randomBoolean(),
                    randomBoolean()
                )
            );
        }
        SharedMinCompetitive.Supplier supplier = new SharedMinCompetitive.Supplier(null, keys);
        return new EsQueryExec.MinCompetitiveSetup(supplier, fieldExists ? "f" : "missingfield");
    }

    private SearchExecutionContext ctx() throws IOException {
        XContentBuilder mapping = mapping();
        class Helper extends MapperServiceTestCase {
            public SearchExecutionContext ctx() throws IOException {
                return createSearchExecutionContext(createMapperService(mapping), searcher);
            }
        }
        return new Helper().ctx();
    }

    private XContentBuilder mapping() throws IOException {
        return MapperServiceTestCase.mapping(builder -> builder.startObject("f").field("type", "long").endObject());
    }

    @Before
    public void setupIndex() throws IOException {
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < NON_NULL_DOCS; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("f", d));
                doc.add(new LongPoint("f", d));
                writer.addDocument(doc);
            }
            for (int d = 0; d < NULL_DOCS; d++) {
                List<IndexableField> doc = new ArrayList<>();
                writer.addDocument(doc);
            }
            reader = writer.getReader();
            searcher = new IndexSearcher(reader);
        }
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }
}
