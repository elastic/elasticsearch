/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class GetNamedCredentialsResponseTests extends ESTestCase {

    public void testSingleResponseRequiresExactlyOneCredential() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GetNamedCredentialsAction.Response(List.of(), true)
        );
        assertThat(e.getMessage(), containsString("exactly one credential"));
    }
}
