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
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;

/**
 *
 */
public class ESMessageFormatTests extends ElasticsearchTestCase {

    @Test
    public void testFormat() throws Exception {
        Object arg1 = randomFrom("arg1", 1, 1.0, new long[] { 1L, 10L }, null);
        Object arg2 = randomFrom("arg2", 2, 2.0, new long[] { 2L, 20L }, null);
        StringBuilder pattern = new StringBuilder();
        StringBuilder expected = new StringBuilder();
        if (randomBoolean()) {
            String text = randomAsciiOfLength(randomIntBetween(1, 10));
            pattern.append(text);
            expected.append(text);
        }
        if (randomBoolean()) {
            pattern.append(" ");
            expected.append(" ");
        }
        if (randomBoolean()) {
            pattern.append("[]");
            if (arg1 instanceof long[]) {
                expected.append(Arrays.toString((long[]) arg1));
            } else {
                expected.append('[').append(String.valueOf(arg1)).append(']');
            }
        } else {
            pattern.append("{}");
            if (arg1 instanceof long[]) {
                expected.append(Arrays.toString((long[]) arg1));
            } else {
                expected.append(String.valueOf(arg1));
            }
        }
        if (randomBoolean()) {
            String text = randomAsciiOfLength(randomIntBetween(1, 10));
            pattern.append(text);
            expected.append(text);
        }
        if (randomBoolean()) {
            pattern.append("[]");
            if (arg2 instanceof long[]) {
                expected.append(Arrays.toString((long[]) arg2));
            } else {
                expected.append('[').append(String.valueOf(arg2)).append(']');
            }
        } else {
            pattern.append("{}");
            if (arg2 instanceof long[]) {
                expected.append(Arrays.toString((long[]) arg2));
            } else {
                expected.append(String.valueOf(arg2));
            }
        }
        if (randomBoolean()) {
            assertThat(ESMessageFormat.format(pattern.toString(), arg1, arg2), is(expected.toString()));
        } else {
            assertThat(ESMessageFormat.formatWithPrefix("prefix-", pattern.toString(), arg1, arg2), is("prefix-" + expected.toString()));
        }
    }

    @Test
    public void testFormatWithoutPlaceholders() throws Exception {
        String pattern = randomAsciiOfLength(10);
        if (randomBoolean()) {
            assertThat(ESMessageFormat.formatWithPrefix("prefix-", pattern), is("prefix-" + pattern));
        } else {
            assertThat(ESMessageFormat.format(pattern), is(pattern));
        }
    }
}
