/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.ql.common.Failure;

import java.util.Collection;

public class VerificationException extends EsqlClientException {

    protected VerificationException(Collection<Failure> sources) {
        super(Failure.failMessage(sources));
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
