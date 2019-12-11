/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.util.EnumSet;

public class OperationModeTests extends ESTestCase {

    public void testIsValidChange() {
        assertFalse(OperationMode.RUNNING.isValidChange(OperationMode.RUNNING));
        assertTrue(OperationMode.RUNNING.isValidChange(OperationMode.STOPPING));
        assertFalse(OperationMode.RUNNING.isValidChange(OperationMode.STOPPED));

        assertTrue(OperationMode.STOPPING.isValidChange(OperationMode.RUNNING));
        assertFalse(OperationMode.STOPPING.isValidChange(OperationMode.STOPPING));
        assertTrue(OperationMode.STOPPING.isValidChange(OperationMode.STOPPED));

        assertTrue(OperationMode.STOPPED.isValidChange(OperationMode.RUNNING));
        assertFalse(OperationMode.STOPPED.isValidChange(OperationMode.STOPPING));
        assertFalse(OperationMode.STOPPED.isValidChange(OperationMode.STOPPED));
    }

    public void testFromName() {
        EnumSet.allOf(OperationMode.class).forEach(e -> assertEquals(OperationMode.fromString(e.name()), e));
    }

    public void testFromNameInvalid() {
        String invalidName = randomAlphaOfLength(10);
        Exception e = expectThrows(IllegalArgumentException.class, () -> OperationMode.fromString(invalidName));
        assertThat(e.getMessage(), CoreMatchers.containsString(invalidName + " is not a valid operation_mode"));
    }
}
