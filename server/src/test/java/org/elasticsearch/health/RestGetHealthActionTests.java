/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class RestGetHealthActionTests extends ESTestCase {

    public void testHealthReportAPIDoesNotTripCircuitBreakers() {
        assertThat(new RestGetHealthAction().canTripCircuitBreaker(), is(false));
    }
}
