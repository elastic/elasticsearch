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
 * Annotation to identify a block of code (a whole class, a method, or a field) that needs to be reviewed (for cleanup, remove or change)
 * before releasing 10.0
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ ElementType.LOCAL_VARIABLE, ElementType.CONSTRUCTOR, ElementType.FIELD, ElementType.METHOD, ElementType.TYPE })
public @interface UpdateForV10 {
    enum Owner {
        CORE_INFRA,
        DATA_MANAGEMENT,
        DISTRIBUTED_COORDINATION,
        DISTRIBUTED_INDEXING,
        ENTERPRISE_SEARCH,
        MACHINE_LEARNING,
        PROFILING,
        SEARCH_ANALYTICS,
        SEARCH_FOUNDATIONS,
        SEARCH_RELEVANCE,
        SECURITY,
    }

    /**
     * The owning team of the task to act on this annotation when the time comes.
     */
    Owner owner();
}
