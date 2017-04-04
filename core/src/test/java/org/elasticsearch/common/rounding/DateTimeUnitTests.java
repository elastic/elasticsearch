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
package org.elasticsearch.common.rounding;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.rounding.DateTimeUnit.DAY_OF_MONTH;
import static org.elasticsearch.common.rounding.DateTimeUnit.HOUR_OF_DAY;
import static org.elasticsearch.common.rounding.DateTimeUnit.MINUTES_OF_HOUR;
import static org.elasticsearch.common.rounding.DateTimeUnit.MONTH_OF_YEAR;
import static org.elasticsearch.common.rounding.DateTimeUnit.QUARTER;
import static org.elasticsearch.common.rounding.DateTimeUnit.SECOND_OF_MINUTE;
import static org.elasticsearch.common.rounding.DateTimeUnit.WEEK_OF_WEEKYEAR;
import static org.elasticsearch.common.rounding.DateTimeUnit.YEAR_OF_CENTURY;

public class DateTimeUnitTests extends ESTestCase {

    /**
     * test that we don't accidentally change enum ids
     */
    public void testEnumIds() {
        assertEquals(1, WEEK_OF_WEEKYEAR.id());
        assertEquals(WEEK_OF_WEEKYEAR, DateTimeUnit.resolve((byte) 1));

        assertEquals(2, YEAR_OF_CENTURY.id());
        assertEquals(YEAR_OF_CENTURY, DateTimeUnit.resolve((byte) 2));

        assertEquals(3, QUARTER.id());
        assertEquals(QUARTER, DateTimeUnit.resolve((byte) 3));

        assertEquals(4, MONTH_OF_YEAR.id());
        assertEquals(MONTH_OF_YEAR, DateTimeUnit.resolve((byte) 4));

        assertEquals(5, DAY_OF_MONTH.id());
        assertEquals(DAY_OF_MONTH, DateTimeUnit.resolve((byte) 5));

        assertEquals(6, HOUR_OF_DAY.id());
        assertEquals(HOUR_OF_DAY, DateTimeUnit.resolve((byte) 6));

        assertEquals(7, MINUTES_OF_HOUR.id());
        assertEquals(MINUTES_OF_HOUR, DateTimeUnit.resolve((byte) 7));

        assertEquals(8, SECOND_OF_MINUTE.id());
        assertEquals(SECOND_OF_MINUTE, DateTimeUnit.resolve((byte) 8));
    }
}
