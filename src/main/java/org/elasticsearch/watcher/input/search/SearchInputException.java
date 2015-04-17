/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import org.elasticsearch.watcher.input.InputException;

/**
 *
 */
public class SearchInputException extends InputException {

    public SearchInputException(String msg, Object... args) {
        super(msg, args);
    }

    public SearchInputException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
