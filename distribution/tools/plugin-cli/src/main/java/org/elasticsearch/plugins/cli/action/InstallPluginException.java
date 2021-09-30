/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli.action;

import org.elasticsearch.plugins.InstallPluginProblem;

public class InstallPluginException extends Exception {
    private final InstallPluginProblem problem;

    public InstallPluginProblem getProblem() {
        return problem;
    }

    public InstallPluginException(InstallPluginProblem problem, String message) {
        super(message);
        this.problem = problem;
    }

    public InstallPluginException(InstallPluginProblem problem, String message, Throwable cause) {
        super(message, cause);
        this.problem = problem;
    }
}
