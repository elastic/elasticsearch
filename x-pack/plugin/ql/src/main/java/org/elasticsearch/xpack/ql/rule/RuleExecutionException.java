/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.rule;

import org.elasticsearch.xpack.ql.QlServerException;

public class RuleExecutionException extends QlServerException {

    public RuleExecutionException(String message, Object... args) {
        super(message, args);
    }
}
