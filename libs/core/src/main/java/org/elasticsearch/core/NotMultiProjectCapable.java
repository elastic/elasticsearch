/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to identify a block of code (a whole class, a method, a field, or a local variable) that is intentionally not fully
 * project-aware because it's not intended to be used in a serverless environment. Some features are unavailable in serverless and are
 * thus not worth the investment to make fully project-aware. This annotation makes it easier to identify blocks of code that require
 * attention in case those features are revisited from a multi-project POV.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(
    { ElementType.LOCAL_VARIABLE, ElementType.CONSTRUCTOR, ElementType.FIELD, ElementType.METHOD, ElementType.TYPE, ElementType.MODULE }
)
public @interface NotMultiProjectCapable {

    /**
     * Some explanation on why the block of code would not work in a multi-project context and/or what would need to be done to make it
     * properly project-aware.
     */
    String description() default "";
}
