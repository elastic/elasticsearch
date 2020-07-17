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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CoreValuesSourceTypeTests extends ESTestCase {

    public void testFromString() {
        assertThat(CoreValuesSourceType.fromString("numeric"), equalTo(CoreValuesSourceType.NUMERIC));
        assertThat(CoreValuesSourceType.fromString("bytes"), equalTo(CoreValuesSourceType.BYTES));
        assertThat(CoreValuesSourceType.fromString("geopoint"), equalTo(CoreValuesSourceType.GEOPOINT));
        assertThat(CoreValuesSourceType.fromString("range"), equalTo(CoreValuesSourceType.RANGE));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CoreValuesSourceType.fromString("does_not_exist"));
        assertThat(e.getMessage(),
            equalTo("No enum constant org.elasticsearch.search.aggregations.support.CoreValuesSourceType.DOES_NOT_EXIST"));
        expectThrows(NullPointerException.class, () -> CoreValuesSourceType.fromString(null));
    }
}
