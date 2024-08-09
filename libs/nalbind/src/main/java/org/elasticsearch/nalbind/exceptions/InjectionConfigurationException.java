/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.exceptions;

public class InjectionConfigurationException extends IllegalStateException {
    public InjectionConfigurationException(String s) {
        super(s);
    }

    public InjectionConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public InjectionConfigurationException(Throwable cause) {
        super(cause);
    }
}
