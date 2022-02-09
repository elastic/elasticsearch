/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

public class URLDecodeProcessorTests extends AbstractStringProcessorTestCase<String> {
    @Override
    protected String modifyInput(String input) {
        return "Hello%20G%C3%BCnter" + input;
    }

    @Override
    protected AbstractStringProcessor<String> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new URLDecodeProcessor(randomAlphaOfLength(10), null, field, ignoreMissing, targetField);
    }

    @Override
    protected String expectedResult(String input) {
        try {
            return "Hello Günter" + URLDecoder.decode(input, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("invalid");
        }
    }

    @Override
    protected boolean isSupportedValue(Object value) {
        // some random strings produced by the randomized test framework contain invalid URL encodings
        if (value instanceof String) {
            return isValidUrlEncodedString((String) value);
        } else if (value instanceof List) {
            for (Object o : (List) value) {
                if ((o instanceof String) == false || isValidUrlEncodedString((String) o) == false) {
                    return false;
                }
            }
            return true;
        } else {
            throw new IllegalArgumentException("unexpected type");
        }
    }

    private static boolean isValidUrlEncodedString(String s) {
        try {
            URLDecoder.decode(s, "UTF-8");
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
