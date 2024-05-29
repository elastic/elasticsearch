/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is meant to be applied to RestHandler classes, and is used to determine which RestHandlers are available to requests
 * at runtime in Serverless mode. This annotation is unused when not running in serverless mode. If this annotation is not present in a
 * RestHandler, then that RestHandler is not available at all in Serverless mode.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ServerlessScope {
    Scope value();

    /**
     * A value used when restricting a response of a serverless endpoints.
     */
    String SERVERLESS_RESTRICTION = "serverless";
}
