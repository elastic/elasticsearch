/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.sql.SqlClientException;

import java.util.Collection;

public class VerificationException extends SqlClientException {

    protected VerificationException(Collection<Failure> sources) {
        super(Failure.failMessage(sources));
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
