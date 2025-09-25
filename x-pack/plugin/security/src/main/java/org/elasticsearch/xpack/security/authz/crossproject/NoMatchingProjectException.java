/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.crossproject;

import org.elasticsearch.ResourceNotFoundException;

/**
 * An exception that a project is missing
 */
public final class NoMatchingProjectException extends ResourceNotFoundException {

    public NoMatchingProjectException(String projectName) {
        super("No such project: [" + projectName + "]");
    }

}
