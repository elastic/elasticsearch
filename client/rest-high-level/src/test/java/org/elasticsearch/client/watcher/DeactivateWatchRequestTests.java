/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class DeactivateWatchRequestTests extends ESTestCase {

    public void testNullId() {
        NullPointerException actual = expectThrows(NullPointerException.class, () -> new DeactivateWatchRequest(null));
        assertNotNull(actual);
        assertThat(actual.getMessage(), is("watch id is missing"));
    }

    public void testInvalidId() {
        IllegalArgumentException actual = expectThrows(IllegalArgumentException.class,
            () -> new DeactivateWatchRequest("Watch id has spaces"));
        assertNotNull(actual);
        assertThat(actual.getMessage(), is("watch id contains whitespace"));
    }

}
