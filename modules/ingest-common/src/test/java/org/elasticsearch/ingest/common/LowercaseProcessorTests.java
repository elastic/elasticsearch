/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import java.util.Locale;

public class LowercaseProcessorTests extends AbstractStringProcessorTestCase<String> {
    @Override
    protected AbstractStringProcessor<String> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new LowercaseProcessor(randomAlphaOfLength(10), null, field, ignoreMissing, targetField);
    }

    @Override
    protected String expectedResult(String input) {
        return input.toLowerCase(Locale.ROOT);
    }
}
