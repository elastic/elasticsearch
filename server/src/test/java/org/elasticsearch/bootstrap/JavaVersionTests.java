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

package org.elasticsearch.bootstrap;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class JavaVersionTests extends ESTestCase {
    public void testParse() {
        JavaVersion javaVersion = JavaVersion.parse("1.7.0");
        List<Integer> version = javaVersion.getVersion();
        assertThat(version.size(), is(3));
        assertThat(version.get(0), is(1));
        assertThat(version.get(1), is(7));
        assertThat(version.get(2), is(0));

        JavaVersion javaVersionEarlyAccess = JavaVersion.parse("14.0.1-ea");
        List<Integer> version14 = javaVersionEarlyAccess.getVersion();
        assertThat(version14.size(), is(3));
        assertThat(version14.get(0), is(14));
        assertThat(version14.get(1), is(0));
        assertThat(version14.get(2), is(1));
        assertTrue(javaVersionEarlyAccess.isEarlyAccess());
    }

    public void testToString() {
        JavaVersion javaVersion170 = JavaVersion.parse("1.7.0");
        assertThat(javaVersion170.toString(), is("1.7.0"));
        JavaVersion javaVersion9 = JavaVersion.parse("9");
        assertThat(javaVersion9.toString(), is("9"));
        JavaVersion javaVersion13ea = JavaVersion.parse("13.1-ea");
        assertThat(javaVersion13ea.toString(), is("13.1-ea"));
    }

    public void testCompare() {
        JavaVersion onePointSix = JavaVersion.parse("1.6");
        JavaVersion onePointSeven = JavaVersion.parse("1.7");
        JavaVersion onePointSevenPointZero = JavaVersion.parse("1.7.0");
        JavaVersion onePointSevenPointOne = JavaVersion.parse("1.7.1");
        JavaVersion onePointSevenPointTwo = JavaVersion.parse("1.7.2");
        JavaVersion onePointSevenPointOnePointOne = JavaVersion.parse("1.7.1.1");
        JavaVersion onePointSevenPointTwoPointOne = JavaVersion.parse("1.7.2.1");
        JavaVersion fourteen = JavaVersion.parse("14");
        JavaVersion fourteenPointTwoPointOne = JavaVersion.parse("14.2.1");
        JavaVersion fourteenPointTwoPointOneEarlyAccess = JavaVersion.parse("14.2.1-ea");

        assertTrue(onePointSix.compareTo(onePointSeven) < 0);
        assertTrue(onePointSeven.compareTo(onePointSix) > 0);
        assertTrue(onePointSix.compareTo(onePointSix) == 0);
        assertTrue(onePointSeven.compareTo(onePointSevenPointZero) == 0);
        assertTrue(onePointSevenPointOnePointOne.compareTo(onePointSevenPointOne) > 0);
        assertTrue(onePointSevenPointTwo.compareTo(onePointSevenPointTwoPointOne) < 0);
        assertTrue(fourteenPointTwoPointOneEarlyAccess.compareTo(fourteenPointTwoPointOne) < 0);
        assertTrue(fourteenPointTwoPointOneEarlyAccess.compareTo(fourteen) > 0);

    }

    public void testValidVersions() {
        String[] versions = new String[]{"1.7", "1.7.0", "0.1.7", "1.7.0.80", "12-ea", "13.0.2.3-ea"};
        for (String version : versions) {
            assertTrue(JavaVersion.isValid(version));
        }
    }

    public void testInvalidVersions() {
        String[] versions = new String[]{"", "1.7.0_80", "1.7."};
        for (String version : versions) {
            assertFalse(JavaVersion.isValid(version));
        }
    }

    public void testJava8Compat() {
        assertEquals(JavaVersion.parse("1.8"), JavaVersion.parse("8"));
    }
}
