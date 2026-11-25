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
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class AnnotatedTextColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new AnnotatedTextPlugin());
    }

    private static Scenario annotatedText() {
        return scenario("annotated_text", () -> randomAlphaOfLengthBetween(1, 8)).withoutIndex();
    }

    public void testAnnotatedTextExistsRegular() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(annotatedText(), Path.REGULAR);
    }

    public void testAnnotatedTextSourceRegular() throws IOException {
        assertSourceDropsNullElements(annotatedText(), Path.REGULAR);
    }

    public void testAnnotatedTextRemainingValuesKeepTheirOrderRegular() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(annotatedText(), Path.REGULAR);
    }
}
