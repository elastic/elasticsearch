/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.qlcore.rule;

import org.elasticsearch.xpack.qlcore.QlServerException;

public class RuleExecutionException extends QlServerException {

    public RuleExecutionException(String message, Object... args) {
        super(message, args);
    }
}
