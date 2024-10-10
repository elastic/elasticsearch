/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;

import java.util.Collection;

public class VerificationException extends EsqlClientException {
    public VerificationException(String message, Object... args) {
        super(message, args);
    }

    public VerificationException(Collection<Failure> sources) {
        super(Failure.failMessage(sources));
    }

    public VerificationException(Failures failures) {
        super(failures.toString());
    }

}
