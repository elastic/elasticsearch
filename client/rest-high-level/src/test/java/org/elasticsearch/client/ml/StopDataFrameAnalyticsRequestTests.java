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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class StopDataFrameAnalyticsRequestTests extends ESTestCase {

    public void testValidate_Ok() {
        assertEquals(Optional.empty(), new StopDataFrameAnalyticsRequest("foo").validate());
        assertEquals(Optional.empty(), new StopDataFrameAnalyticsRequest("foo").setTimeout(null).validate());
        assertEquals(Optional.empty(), new StopDataFrameAnalyticsRequest("foo").setTimeout(TimeValue.ZERO).validate());
    }

    public void testValidate_Failure() {
        assertThat(new StopDataFrameAnalyticsRequest(null).validate().get().getMessage(),
            containsString("data frame analytics id must not be null"));
        assertThat(new StopDataFrameAnalyticsRequest(null).setTimeout(TimeValue.ZERO).validate().get().getMessage(),
            containsString("data frame analytics id must not be null"));
    }
}
