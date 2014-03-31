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

package org.elasticsearch.common;

import org.elasticsearch.test.ElasticsearchTestCase;
import static org.hamcrest.Matchers.*;
import org.junit.Test;

public class BooleansTests extends ElasticsearchTestCase {

    @Test
    public void testIsBoolean() {
        String[] booleans = new String[]{"true", "false", "on", "off", "yes", "no", "0", "1", "F", "T"};
        String[] notBooleans = new String[]{"11", "00", "sdfsdfsf"};

        for (String b : booleans) {
            String t = "prefix" + b + "suffix";
            assertThat("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()), equalTo(true));
        }

        for (String nb : notBooleans) {
            String t = "prefix" + nb + "suffix";
            assertThat("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()), equalTo(false));
        }
    }

    @Test
    public void testParseBoolean() {
        String[] trueValues = new String[]{"true", "on", "yes", "1", "T"};
        String[] falseValues = new String[]{"false", "off", "no", "0", "F", ""};
        String[] unknownValues = new String[]{"11", "00", "sdfsdfsf"};  // unknown value should be treat as true

        for (String s : trueValues) {
            assertThat(Booleans.parseBoolean(s, false), equalTo(true));
            assertThat(Booleans.parseBoolean(s, null), equalTo(true));
            assertThat(Booleans.parseBoolean(s.toCharArray(), 0, s.length(), false), equalTo(true));
        }

        for (String s : falseValues) {
            assertThat(Booleans.parseBoolean(s, false), equalTo(false));
            assertThat(Booleans.parseBoolean(s, null), equalTo(false));
            assertThat(Booleans.parseBoolean(s.toCharArray(), 0, s.length(), false), equalTo(false));
        }

        for (String s : unknownValues) {
            assertThat(Booleans.parseBoolean(s, false), equalTo(true));
            assertThat(Booleans.parseBoolean(s, null), equalTo(true));
            assertThat(Booleans.parseBoolean(s.toCharArray(), 0, s.length(), false), equalTo(true));
        }
    }
}
