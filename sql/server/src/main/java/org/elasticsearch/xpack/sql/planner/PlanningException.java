/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.sql.ClientSqlException;
import org.elasticsearch.xpack.sql.planner.Verifier.Failure;

import java.util.Collection;
import java.util.StringJoiner;

public class PlanningException extends ClientSqlException {

    public PlanningException(String message, Object... args) {
        super(message, args);
    }

    public PlanningException(Collection<Failure> sources) {
        super(extractMessage(sources));
    }

    private static String extractMessage(Collection<Failure> sources) {
        StringJoiner sj = new StringJoiner(",", "{", "}");
        sources.forEach(s -> {
            sj.add(s.source().nodeString() + s.source().location());
        });
        return "Fail to plan items " + sj.toString();
    }
}
