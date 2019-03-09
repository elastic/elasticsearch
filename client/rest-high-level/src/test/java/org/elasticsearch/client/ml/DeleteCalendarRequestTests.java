/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
