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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.index.fielddata.ScriptDocValues.Dates;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.hasItems;

public class ScriptDocValuesDatesTests extends ESTestCase {

    public void testJavaTime() throws IOException {
        assertDateDocValues(true, "getDate is no longer necessary on date fields as the value is now a date.",
            "getDates is no longer necessary on date fields as the values are now dates.");
    }

    public void testJodaTimeBwc() throws IOException {
        assertDateDocValues(false, "The joda time api for doc values is deprecated." +
            " Use -Des.scripting.use_java_time=true to use the java time api for date field doc values",
            "getDate is no longer necessary on date fields as the value is now a date.",
            "getDates is no longer necessary on date fields as the values are now dates.");
    }

    public void assertDateDocValues(boolean useJavaTime, String... expectedWarnings) throws IOException {
        final Function<Long, Object> datetimeCtor;
        if (useJavaTime) {
            datetimeCtor = millis -> ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
        } else {
            datetimeCtor = millis -> new DateTime(millis, DateTimeZone.UTC);
        }
        long[][] values = new long[between(3, 10)][];
        Object[][] expectedDates = new Object[values.length][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            expectedDates[d] = new Object[values[d].length];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomNonNegativeLong();
                expectedDates[d][i] = datetimeCtor.apply(values[d][i]);
            }
        }

        Set<String> warnings = new HashSet<>();
        Dates dates = wrap(values, (key, deprecationMessage) -> {
            warnings.add(deprecationMessage);
            /* Create a temporary directory to prove we are running with the
             * server's permissions. */
            createTempDir();
        }, useJavaTime);
        // each call to get or getValue will be run with limited permissions, just as they are in scripts
        PermissionCollection noPermissions = new Permissions();
        AccessControlContext noPermissionsAcc = new AccessControlContext(
            new ProtectionDomain[] {
                new ProtectionDomain(null, noPermissions)
            }
        );

        boolean valuesExist = false;
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            dates.setNextDocId(d);
            Object dateValue = AccessController.doPrivileged((PrivilegedAction<Object>) dates::getValue, noPermissionsAcc);
            assertEquals(expectedDates[d].length > 0 ? expectedDates[d][0] : new DateTime(0, DateTimeZone.UTC), dateValue);
            Object bwcDateValue = AccessController.doPrivileged((PrivilegedAction<Object>) dates::getDate, noPermissionsAcc);
            assertEquals(expectedDates[d].length > 0 ? expectedDates[d][0] : new DateTime(0, DateTimeZone.UTC), bwcDateValue);
            valuesExist |= expectedDates[d].length > 0;

            AccessController.doPrivileged((PrivilegedAction<Object>) dates::getDates, noPermissionsAcc);
            assertEquals(values[d].length, dates.size());
            for (int i = 0; i < values[d].length; i++) {
                final int ndx = i;
                Object dateValueI = AccessController.doPrivileged((PrivilegedAction<Object>) () -> dates.get(ndx), noPermissionsAcc);
                assertEquals(expectedDates[d][i], dateValueI);
            }
        }
        // using "hasItems" here instead of "containsInAnyOrder",
        // because values are randomly initialized, sometimes some of docs will not have any values
        // and warnings in this case will contain another deprecation warning on missing values
        if (valuesExist) {
            assertThat(warnings, hasItems(expectedWarnings));
        }
    }

    private Dates wrap(long[][] values, BiConsumer<String, String> deprecationHandler, boolean useJavaTime) {
        return new Dates(new AbstractSortedNumericDocValues() {
            long[] current;
            int i;

            @Override
            public boolean advanceExact(int doc) {
                current = values[doc];
                i = 0;
                return current.length > 0;
            }
            @Override
            public int docValueCount() {
                return current.length;
            }
            @Override
            public long nextValue() {
                return current[i++];
            }
        }, deprecationHandler, useJavaTime);
    }
}
