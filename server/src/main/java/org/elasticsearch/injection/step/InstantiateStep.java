/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection.step;

import org.elasticsearch.injection.spec.MethodHandleSpec;

/**
 * Constructs a new object by invoking a {@link java.lang.invoke.MethodHandle}
 * as specified by a given {@link MethodHandleSpec}.
 */
public record InstantiateStep(MethodHandleSpec spec) implements InjectionStep {}
