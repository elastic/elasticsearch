/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import java.util.regex.Pattern;

public class GsubProcessorTests extends AbstractStringProcessorTestCase<String> {

    @Override
    protected AbstractStringProcessor<String> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new GsubProcessor(randomAlphaOfLength(10), null, field, Pattern.compile("\\."), "-", ignoreMissing, targetField);
    }

    @Override
    protected String modifyInput(String input) {
        return "127.0.0.1";
    }

    @Override
    protected String expectedResult(String input) {
        return "127-0-0-1";
    }
}
