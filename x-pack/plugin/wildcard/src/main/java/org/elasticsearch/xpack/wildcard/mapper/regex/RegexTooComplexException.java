/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * Wraps Lucene's XTooComplexToDeterminizeException to be serializable to be
 * thrown over the wire.
 */
public class RegexTooComplexException extends RuntimeException {

    public RegexTooComplexException(TooComplexToDeterminizeException e) {
        super(e.getMessage());
    }
}
