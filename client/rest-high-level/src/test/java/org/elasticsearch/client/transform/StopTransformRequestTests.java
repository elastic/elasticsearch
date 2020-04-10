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

package org.elasticsearch.client.transform;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class StopTransformRequestTests extends ESTestCase {
    public void testValidate_givenNullId() {
        StopTransformRequest request = new StopTransformRequest(null);
        Optional<ValidationException> validate = request.validate();
        assertTrue(validate.isPresent());
        assertThat(validate.get().getMessage(), containsString("transform id must not be null"));
    }

    public void testValidate_givenValid() {
        StopTransformRequest request = new StopTransformRequest("foo");
        Optional<ValidationException> validate = request.validate();
        assertFalse(validate.isPresent());
    }
}
