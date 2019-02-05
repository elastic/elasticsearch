/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.sql.ClientSqlException;
import org.elasticsearch.xpack.sql.planner.Verifier.Failure;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Collection;
import java.util.stream.Collectors;

public class PlanningException extends ClientSqlException {
    public PlanningException(String message, Object... args) {
        super(message, args);
    }

    public PlanningException(Collection<Failure> sources) {
        super(extractMessage(sources));
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    private static String extractMessage(Collection<Failure> failures) {
        return failures.stream()
                .map(f -> {
                    Location l = f.source().source().source();
                    return "line " + l.getLineNumber() + ":" + l.getColumnNumber() + ": " + f.message();
                })
                .collect(Collectors.joining("\n", "Found " + failures.size() + " problem(s)\n", ""));
    }
}
