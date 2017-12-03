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

package org.elasticsearch.common.joda;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;


public class JodaTests extends ESTestCase {


    public void testBasicTTimePattern() {
        FormatDateTimeFormatter formatter1 = Joda.forPattern("basic_t_time");
        assertEquals(formatter1.format(), "basic_t_time");
        DateTimeFormatter parser1 = formatter1.parser();

        assertEquals(parser1.getZone(), DateTimeZone.UTC);

        FormatDateTimeFormatter formatter2 = Joda.forPattern("basicTTime");
        assertEquals(formatter2.format(), "basicTTime");
        DateTimeFormatter parser2 = formatter2.parser();

        assertEquals(parser2.getZone(), DateTimeZone.UTC);

        DateTime dt = new DateTime(2004, 6, 9, 10, 20, 30, 40, DateTimeZone.UTC);
        assertEquals("T102030.040Z", parser1.print(dt));
        assertEquals("T102030.040Z", parser2.print(dt));

        expectThrows(IllegalArgumentException.class, () -> Joda.forPattern("basic_t_Time"));
        expectThrows(IllegalArgumentException.class, () -> Joda.forPattern("basic_T_Time"));
        expectThrows(IllegalArgumentException.class, () -> Joda.forPattern("basic_T_time"));
    }

}
