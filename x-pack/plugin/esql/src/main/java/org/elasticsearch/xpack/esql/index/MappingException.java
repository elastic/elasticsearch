/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.xpack.esql.core.QlClientException;

public class MappingException extends QlClientException {

    public MappingException(String message, Object... args) {
        super(message, args);
    }

}
