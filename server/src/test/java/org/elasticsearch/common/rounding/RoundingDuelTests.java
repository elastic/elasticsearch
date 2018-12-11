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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.time.ZoneOffset;

import static org.hamcrest.Matchers.is;

public class RoundingDuelTests extends ESTestCase  {

    // dont include nano/micro seconds as rounding would become zero then and throw an exception
    private static final String[] ALLOWED_TIME_SUFFIXES = new String[]{"d", "h", "ms", "s", "m"};

    public void testSerialization() throws Exception {
        org.elasticsearch.common.Rounding.DateTimeUnit randomDateTimeUnit =
            randomFrom(org.elasticsearch.common.Rounding.DateTimeUnit.values());
        org.elasticsearch.common.Rounding rounding;
        if (randomBoolean()) {
            rounding = org.elasticsearch.common.Rounding.builder(randomDateTimeUnit).timeZone(ZoneOffset.UTC).build();
        } else {
            rounding = org.elasticsearch.common.Rounding.builder(timeValue()).timeZone(ZoneOffset.UTC).build();
        }
        BytesStreamOutput output = new BytesStreamOutput();
        rounding.writeTo(output);

        Rounding roundingJoda = Rounding.Streams.read(output.bytes().streamInput());
        org.elasticsearch.common.Rounding roundingJavaTime =
            org.elasticsearch.common.Rounding.read(output.bytes().streamInput());

        int randomInt = randomIntBetween(1, 1_000_000_000);
        assertThat(roundingJoda.round(randomInt), is(roundingJavaTime.round(randomInt)));
        assertThat(roundingJoda.nextRoundingValue(randomInt), is(roundingJavaTime.nextRoundingValue(randomInt)));
    }

    static TimeValue timeValue() {
        return TimeValue.parseTimeValue(randomIntBetween(1, 1000) + randomFrom(ALLOWED_TIME_SUFFIXES), "settingName");
    }
}
