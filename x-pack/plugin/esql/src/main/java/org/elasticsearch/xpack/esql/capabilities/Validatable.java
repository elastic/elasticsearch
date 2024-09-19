/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.common.Failures;

/**
 * Interface implemented by expressions that require validation post logical optimization,
 * when the plan and references have been not just resolved but also replaced.
 */
public interface Validatable {

    /**
     * Validates the implementing expression - discovered failures are reported to the given
     * {@link Failures} class.
     */
    default void validate(Failures failures) {}
}
