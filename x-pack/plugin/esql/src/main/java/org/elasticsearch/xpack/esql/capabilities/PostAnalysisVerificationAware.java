/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.plan.logical.Filter;

/**
 * Interface implemented by expressions or plans that require validation after query plan analysis,
 * when the indices and references have been resolved, but before the plan is transformed further by optimizations.
 * The interface is similar to {@link PostAnalysisPlanVerificationAware}, but focused on individual expressions or plans, typically
 * covering syntactic checks.
 */
public interface PostAnalysisVerificationAware {

    /**
     * Allows the implementer to validate itself. This usually involves checking its internal setup, which often means checking the
     * parameters it received on construction: their data or syntactic type, class, their count, expressions' structure etc.
     * The discovered failures are added to the given {@link Failures} object.
     * <p>
     * It is often more useful to perform the checks as extended as it makes sense, over stopping at the first failure. This will allow the
     * author to progress faster to a correct query.
     * </p>
     * <p>
     *     Example: the {@link Filter} class, which models the WHERE command, checks that the expression it filters on - {@code condition}
     *     - is of a Boolean or NULL type:
     *     <pre>
     *     {@code
     *     @Override
     *     void postAnalysisVerification(Failures failures) {
     *          if (condition.dataType() != NULL && condition.dataType() != BOOLEAN) {
     *              failures.add(fail(condition, "Condition expression needs to be boolean, found [{}]", condition.dataType()));
     *          }
     *     }
     *     }
     *     </pre>
     *
     * @param failures the object to add failures to.
     */
    void postAnalysisVerification(Failures failures);
}
