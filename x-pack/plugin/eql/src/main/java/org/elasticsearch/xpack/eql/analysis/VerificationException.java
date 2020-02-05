/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.eql.EqlClientException;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Collection;
import java.util.stream.Collectors;

public class VerificationException extends EqlClientException {

    protected VerificationException(Collection<Failure> sources) {
        super(asMessage(sources));
    }

    private static String asMessage(Collection<Failure> failures) {
        return failures.stream().map(f -> {
            Location l = f.node().source().source();
            return "line " + l.getLineNumber() + ":" + l.getColumnNumber() + ": " + f.message();
        }).collect(Collectors.joining(StringUtils.NEW_LINE, "Found " + failures.size() + " problem(s)\n", StringUtils.EMPTY));
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
