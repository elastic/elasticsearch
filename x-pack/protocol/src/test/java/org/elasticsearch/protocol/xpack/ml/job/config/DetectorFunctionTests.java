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
package org.elasticsearch.protocol.xpack.ml.job.config;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DetectorFunctionTests extends ESTestCase {

    public void testShortcuts() {
        assertThat(DetectorFunction.fromString("nzc").getFullName(), equalTo("non_zero_count"));
        assertThat(DetectorFunction.fromString("low_nzc").getFullName(), equalTo("low_non_zero_count"));
        assertThat(DetectorFunction.fromString("high_nzc").getFullName(), equalTo("high_non_zero_count"));
        assertThat(DetectorFunction.fromString("dc").getFullName(), equalTo("distinct_count"));
        assertThat(DetectorFunction.fromString("low_dc").getFullName(), equalTo("low_distinct_count"));
        assertThat(DetectorFunction.fromString("high_dc").getFullName(), equalTo("high_distinct_count"));
    }

    public void testFromString_GivenInvalidFunction() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DetectorFunction.fromString("invalid"));
        assertThat(e.getMessage(), equalTo("Unknown function 'invalid'"));
    }
}
