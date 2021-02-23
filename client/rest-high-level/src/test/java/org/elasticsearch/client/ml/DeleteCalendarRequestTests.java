/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;


public class DeleteCalendarRequestTests extends ESTestCase {

    public void testWithNullId() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new DeleteCalendarRequest(null));
        assertEquals("[calendar_id] must not be null", ex.getMessage());
    }

    public void testEqualsAndHash() {
        String id1 = randomAlphaOfLength(8);
        String id2 = id1 + "_a";
        assertThat(new DeleteCalendarRequest(id1), equalTo(new DeleteCalendarRequest(id1)));
        assertThat(new DeleteCalendarRequest(id1).hashCode(), equalTo(new DeleteCalendarRequest(id1).hashCode()));
        assertThat(new DeleteCalendarRequest(id1), not(equalTo(new DeleteCalendarRequest(id2))));
        assertThat(new DeleteCalendarRequest(id1).hashCode(), not(equalTo(new DeleteCalendarRequest(id2).hashCode())));
    }
}
