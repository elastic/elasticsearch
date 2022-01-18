/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

public class NonDeterministicAnnotation {

    public static final String NAME = "nondeterministic";

    public static final NonDeterministicAnnotation INSTANCE = new NonDeterministicAnnotation();

    private NonDeterministicAnnotation() {}
}
