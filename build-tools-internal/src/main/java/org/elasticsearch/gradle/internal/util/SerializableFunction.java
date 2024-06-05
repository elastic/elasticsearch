/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A functional interface that extends Function but also Serializable.
 *
 * Gradle configuration cache requires fields that represent a lambda to be serializable.
 * */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
