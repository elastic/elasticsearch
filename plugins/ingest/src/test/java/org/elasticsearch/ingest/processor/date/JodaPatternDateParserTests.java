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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.hamcrest.core.IsEqual.equalTo;

public class JodaPatternDateParserTests extends ESTestCase {

    public void testParse() {
        JodaPatternDateParser parser = new JodaPatternDateParser("MMM dd HH:mm:ss Z", Locale.ENGLISH);

        assertThat(Instant.ofEpochMilli(parser.parseMillis("Nov 24 01:29:01 -0800"))
                .atZone(ZoneId.of("GMT-8"))
                .format(DateTimeFormatter.ofPattern("MM dd HH:mm:ss")),
                        equalTo("11 24 01:29:01"));
    }
}
