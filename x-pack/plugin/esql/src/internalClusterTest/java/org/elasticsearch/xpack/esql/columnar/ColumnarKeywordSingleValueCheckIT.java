/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * A filter on a keyword is pushed to Lucene beside a check that the field holds one value in the document. A columnar
 * keyword whose every document holds one value says so, and the check is dropped, which leaves the filter to the
 * column alone. Holds whether or not the mapping promises single values.
 */
// The production per-field codec, so the keyword is a ColumNAR column. ESIntegTestCase otherwise picks a codec that bypasses it.
@SuppressCodecs("*")
public class ColumnarKeywordSingleValueCheckIT extends AbstractEsqlIntegTestCase {

    private static final String CHECK = "single_value_match";

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testDroppedWhenEveryDocumentHoldsOneValue() {
        for (boolean multiValue : new boolean[] { true, false }) {
            final String index = "dense_" + multiValue;
            createIndex(index, multiValue);
            final int docs = between(20, 200);
            for (int doc = 0; doc < docs; doc++) {
                prepareIndex(index).setSource("kw", "term-" + (doc % 5)).get();
            }
            flushAndMerge(index);
            assertThat(index, pushedQueries(index), everyItem(not(containsString(CHECK))));
        }
    }

    /** A document without the field holds no value, so the check has documents to reject and stays. */
    public void testKeptWhenADocumentHoldsNoValue() {
        for (boolean multiValue : new boolean[] { true, false }) {
            final String index = "sparse_" + multiValue;
            createIndex(index, multiValue);
            final int docs = between(20, 200);
            for (int doc = 0; doc < docs; doc++) {
                if (doc % 4 == 0) {
                    prepareIndex(index).setSource("other", doc).get();
                } else {
                    prepareIndex(index).setSource("kw", "term-" + (doc % 5)).get();
                }
            }
            flushAndMerge(index);
            assertThat(index, pushedQueries(index), hasItem(containsString(CHECK)));
        }
    }

    private void createIndex(String index, boolean multiValue) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), multiValue)
                    .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("kw", "type=keyword", "other", "type=long")
        );
    }

    private void flushAndMerge(String index) {
        indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).setFlush(true).get();
        indicesAdmin().prepareRefresh(index).get();
    }

    /** The Lucene queries a filter on the keyword ran as, after rewriting. */
    private Set<String> pushedQueries(String index) {
        final EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest("FROM " + index + " | WHERE kw != \"term-1\" | KEEP kw");
        request.profile(true);
        request.pragmas(getPragmas());
        final Set<String> queries = new TreeSet<>();
        try (EsqlQueryResponse response = run(request)) {
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof LuceneSourceOperator.Status status) {
                        queries.addAll(status.processedQueries());
                    }
                }
            }
        }
        assertThat("the filter was pushed to Lucene", queries.isEmpty(), equalTo(false));
        return queries;
    }
}
