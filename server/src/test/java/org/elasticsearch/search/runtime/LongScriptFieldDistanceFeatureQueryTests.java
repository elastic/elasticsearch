/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldDistanceFeatureQueryTests extends AbstractScriptFieldQueryTestCase<LongScriptFieldDistanceFeatureQuery> {
    private final Function<LeafReaderContext, AbstractLongFieldScript> leafFactory = ctx -> null;

    @Override
    protected LongScriptFieldDistanceFeatureQuery createTestInstance() {
        long origin = randomLong();
        long pivot = randomValueOtherThan(origin, ESTestCase::randomLong);
        return new LongScriptFieldDistanceFeatureQuery(randomScript(), leafFactory, randomAlphaOfLength(5), origin, pivot);
    }

    @Override
    protected LongScriptFieldDistanceFeatureQuery copy(LongScriptFieldDistanceFeatureQuery orig) {
        return new LongScriptFieldDistanceFeatureQuery(orig.script(), leafFactory, orig.fieldName(), orig.origin(), orig.pivot());
    }

    @Override
    protected LongScriptFieldDistanceFeatureQuery mutate(LongScriptFieldDistanceFeatureQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        long origin = orig.origin();
        long pivot = orig.pivot();
        switch (randomInt(3)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                origin = randomValueOtherThan(origin, () -> randomValueOtherThan(orig.pivot(), ESTestCase::randomLong));
                break;
            case 3:
                pivot = randomValueOtherThan(origin, () -> randomValueOtherThan(orig.pivot(), ESTestCase::randomLong));
                break;
            default:
                fail();
        }
        return new LongScriptFieldDistanceFeatureQuery(script, leafFactory, fieldName, origin, pivot);
    }

    @Override
    public void testMatches() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181354]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181351]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchLookup searchLookup = new SearchLookup(null, null);
                Function<LeafReaderContext, AbstractLongFieldScript> leafFactory = ctx -> new DateFieldScript(
                    "test",
                    Map.of(),
                    searchLookup,
                    null,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        for (Object timestamp : (List<?>) searchLookup.source().get("timestamp")) {
                            emit(((Number) timestamp).longValue());
                        }
                    }
                };
                LongScriptFieldDistanceFeatureQuery query = new LongScriptFieldDistanceFeatureQuery(
                    randomScript(),
                    leafFactory,
                    "test",
                    1595432181351L,
                    3L
                );
                TopDocs td = searcher.search(query, 2);
                assertThat(td.scoreDocs[0].score, equalTo(1.0f));
                assertThat(td.scoreDocs[0].doc, equalTo(1));
                assertThat(td.scoreDocs[1].score, equalTo(.5f));
                assertThat(td.scoreDocs[1].doc, equalTo(0));
            }
        }
    }

    public void testMaxScore() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of());
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LongScriptFieldDistanceFeatureQuery query = createTestInstance();
                float boost = randomFloat();
                assertThat(
                    query.createWeight(searcher, ScoreMode.COMPLETE, boost).scorer(reader.leaves().get(0)).getMaxScore(randomInt()),
                    equalTo(boost)
                );
            }
        }
    }

    @Override
    protected void assertToString(LongScriptFieldDistanceFeatureQuery query) {
        assertThat(
            query.toString(query.fieldName()),
            equalTo("LongScriptFieldDistanceFeatureQuery(origin=" + query.origin() + ",pivot=" + query.pivot() + ")")
        );
    }

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
