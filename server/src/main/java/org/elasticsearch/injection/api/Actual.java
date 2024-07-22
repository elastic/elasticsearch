/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection.api;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks an injected parameter to indicate that it must not be proxied.
 * This allows methods to be called on the object, and methods like
 * <code>getClass</code>, as well as identity comparison, will all work as expected.
 * <p>
 * Establishes an initialization ordering dependency: the injected object
 * must be initialized first.
 * <p>
 * Without this annotation, the injected object may actually be a proxy,
 * to be initialized later.
 * The upside of proxies is that there's no problem with circular dependencies,
 * but the downside is that methods cannot be called on the injected object.
 */
@Target(PARAMETER)
@Retention(RUNTIME)
public @interface Actual {
}
