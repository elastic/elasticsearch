/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

/**
 * An {@code expect_failure:} block on a variant: querying it must fail with a clean, attributable
 * client error — never a 5xx, never a hang, never silently-wrong rows. Exercised by the sibling
 * expected-failure IT (csv-spec cannot express "this query must fail").
 *
 * @param status       expected status class, e.g. {@code 4xx} (any 400-499) or an exact code
 * @param messageRegex regex the error message must match (case handled by the pattern itself)
 * @param reason       why this misconfiguration must be rejected — surfaces in assertion messages
 */
public record FailureSpec(String status, String messageRegex, String reason) {

    /** Whether {@code actualStatus} satisfies the declared expectation. */
    public boolean statusMatches(int actualStatus) {
        if ("4xx".equalsIgnoreCase(status)) {
            return actualStatus >= 400 && actualStatus < 500;
        }
        return Integer.toString(actualStatus).equals(status);
    }
}
