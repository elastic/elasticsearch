/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.annotatedtext;

import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class AnnotatedTextColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new AnnotatedTextPlugin());
    }

    // NOTE: annotated_text does not implement batch indexing today, so the scenario lists no batched shape and the BATCH tests are not
    // required to execute as a batch. They exercise batch indexing as soon as the mapper accepts a column.
    private static Scenario annotatedText() {
        return scenario("annotated_text", () -> randomAlphaOfLengthBetween(1, 8)).withDefaultIndexSetting();
    }

    public void testAnnotatedTextExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(annotatedText(), IndexingStrategy.DOCUMENT);
    }

    public void testAnnotatedTextSourceModesAgreeDocument() throws IOException {
        assertSourceModesAgree(annotatedText(), IndexingStrategy.DOCUMENT);
    }

    public void testAnnotatedTextSourceModesAgreeBatch() throws IOException {
        assertSourceModesAgree(annotatedText(), IndexingStrategy.BATCH);
    }

    public void testAnnotatedTextExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(annotatedText(), IndexingStrategy.BATCH);
    }

    public void testAnnotatedTextSourceDocument() throws IOException {
        assertSourceDropsNullElements(annotatedText(), IndexingStrategy.DOCUMENT);
    }

    public void testAnnotatedTextSourceBatch() throws IOException {
        assertSourceDropsNullElements(annotatedText(), IndexingStrategy.BATCH);
    }

    public void testAnnotatedTextRemainingValuesKeepTheirOrderDocument() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(annotatedText(), IndexingStrategy.DOCUMENT);
    }

    public void testAnnotatedTextRemainingValuesKeepTheirOrderBatch() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(annotatedText(), IndexingStrategy.BATCH);
    }
}
