/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.Collections;
import java.util.List;

/**
 * Inject compiler setting constants.
 * Format: {@code inject_constant["1=foo_compiler_setting", 2="bar_compiler_setting"]} injects "foo_compiler_setting and
 * "bar_compiler_setting" as the first two arguments (other than receiver reference for instance methods) to the annotated method.
 */
public record InjectConstantAnnotation(List<String> injects) {
    public static final String NAME = "inject_constant";

    public InjectConstantAnnotation(List<String> injects) {
        this.injects = Collections.unmodifiableList(injects);
    }
}
