/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.xpack.ql.QlClientException;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.tree.Location;

import java.util.Collection;
import java.util.List;

public class EqlClientException extends QlClientException {

    private final List<Problem> problems;

    public EqlClientException(String message, Object... args) {
        super(message, args);
        this.problems = List.of();
    }

    protected EqlClientException(String message, Throwable cause) {
        super(message, cause);
        this.problems = List.of();
    }

    protected EqlClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
        this.problems = List.of();
    }

    protected EqlClientException(Collection<Failure> failures) {
        super(Failure.failMessage(failures));
        this.problems = failures.stream().map(EqlClientException::problem).toList();
    }

    public List<Problem> problems() {
        return problems;
    }

    private static Problem problem(Failure failure) {
        Location location = failure.node().source().source();
        return new Problem(location.getLineNumber(), location.getColumnNumber(), failure.message());
    }

    public record Problem(int line, int column, String message) {}

}
