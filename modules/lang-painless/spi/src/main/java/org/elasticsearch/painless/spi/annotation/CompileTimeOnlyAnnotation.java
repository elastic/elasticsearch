/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

/**
 * Methods annotated with this must be run at compile time so their arguments
 * must all be constant and they produce a constant.
 */
public class CompileTimeOnlyAnnotation {
    public static final String NAME = "compile_time_only";

    public static final CompileTimeOnlyAnnotation INSTANCE = new CompileTimeOnlyAnnotation();

    private CompileTimeOnlyAnnotation() {}
}
