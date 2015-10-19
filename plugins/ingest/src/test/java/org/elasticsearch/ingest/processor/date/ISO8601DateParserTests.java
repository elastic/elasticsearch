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

package org.elasticsearch.ingest.processor.date;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;

public class ISO8601DateParserTests extends ESTestCase {

    public void testParseUTC() {
        ISO8601DateParser parser = new ISO8601DateParser(DateTimeZone.UTC);
        assertThat(parser.parseMillis("2001-01-01T00:00:00-0800"), equalTo(978336000000L));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseFailure() {
        ISO8601DateParser parser = new ISO8601DateParser(DateTimeZone.UTC);
        parser.parseMillis("2001-01-0:00-0800");
    }
}
