/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier.Failure;

import java.util.Collection;
import java.util.stream.Collectors;


public class VerificationException extends AnalysisException {

    private final Collection<Failure> failures;

    protected VerificationException(Collection<Failure> sources) {
        super(null, StringUtils.EMPTY);
        failures = sources;
    }

    @Override
    public String getMessage() {
        return failures.stream()
                .map(f -> {
                    Location l = f.node().source().source();
                    return "line " + l.getLineNumber() + ":" + l.getColumnNumber() + ": " + f.message();
                })
                .collect(Collectors.joining(StringUtils.NEW_LINE, "Found " + failures.size() + " problem(s)\n", StringUtils.EMPTY));
    }
}
