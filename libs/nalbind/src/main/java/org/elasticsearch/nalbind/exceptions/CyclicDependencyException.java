/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.exceptions;

import java.util.ArrayList;
import java.util.List;

public class CyclicDependencyException extends InjectionConfigurationException {
    public final List<String> dependencySteps = new ArrayList<>();

    public CyclicDependencyException(String s) {
        super(s);
        dependencySteps.add(s);
    }

    public CyclicDependencyException(String message, Throwable cause) {
        super(message, cause);
        dependencySteps.add(message);
    }

}
