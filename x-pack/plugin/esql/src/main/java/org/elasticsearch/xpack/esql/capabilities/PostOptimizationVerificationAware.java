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
public interface PostOptimizationVerificationAware {

    /**
     * Validates the implementing expression - discovered failures are reported to the given
     * {@link Failures} class.
     *
     * <p>
     *     Example: the {@code Bucket} function, which produces buckets over a numerical or date field, based on a number of literal
     *     arguments needs to check if its arguments are all indeed literals. This is how this verification is performed:
     *     <pre>
     *     {@code
     *
     *      @Override
     *      public void postOptimizationVerification(Failures failures) {
     *          String operation = sourceText();
     *
     *          failures.add(isFoldable(buckets, operation, SECOND))
     *              .add(from != null ? isFoldable(from, operation, THIRD) : null)
     *              .add(to != null ? isFoldable(to, operation, FOURTH) : null);
     *      }
     *     }
     *     </pre>
     *
     */
    void postOptimizationVerification(Failures failures);
}
