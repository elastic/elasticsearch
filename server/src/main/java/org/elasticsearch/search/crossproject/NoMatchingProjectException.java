/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * An exception that a project is missing
 */
public final class NoMatchingProjectException extends ResourceNotFoundException {

    public NoMatchingProjectException(String projectName) {
        super("No such project: [" + projectName + "]");
    }

    public NoMatchingProjectException(StreamInput in) throws IOException {
        super(in);
    }

}
