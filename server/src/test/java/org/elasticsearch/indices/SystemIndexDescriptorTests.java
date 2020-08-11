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

package org.elasticsearch.indices;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class SystemIndexDescriptorTests extends ESTestCase {

    public void testValidation() {
        {
            Exception ex = expectThrows(NullPointerException.class,
                () -> new SystemIndexDescriptor(null, randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not be null"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor("", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(randomAlphaOfLength(10), randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must start with the character [.]"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*" + randomAlphaOfLength(10), randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
    }
}
