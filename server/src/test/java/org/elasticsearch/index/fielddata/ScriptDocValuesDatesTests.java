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
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
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
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;

public class ScriptDocValuesDatesTests extends ESTestCase {

    public void test() throws IOException {
        assertDateDocValues("getDate is no longer necessary on date fields as the value is now a date.",
            "getDates is no longer necessary on date fields as the values are now dates.");
    }

    public void assertDateDocValues(String... expectedWarnings) throws IOException {
        long[][] values = new long[between(3, 10)][];
        JodaCompatibleZonedDateTime[][] expectedDates = new JodaCompatibleZonedDateTime[values.length][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            expectedDates[d] = new JodaCompatibleZonedDateTime[values[d].length];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomNonNegativeLong();
                expectedDates[d][i] = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(values[d][i]), ZoneOffset.UTC);
            }
        }

        Set<String> warnings = new HashSet<>();
        Dates dates = wrap(values, (key, deprecationMessage) -> {
            warnings.add(deprecationMessage);
            /* Create a temporary directory to prove we are running with the
             * server's permissions. */
            createTempDir();
        });
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
            if (expectedDates[d].length > 0) {
                JodaCompatibleZonedDateTime dateValue =
                    AccessController.doPrivileged((PrivilegedAction<JodaCompatibleZonedDateTime>) dates::getValue, noPermissionsAcc);
                assertEquals(expectedDates[d].length > 0 ? expectedDates[d][0] : new DateTime(0, DateTimeZone.UTC), dateValue);
                JodaCompatibleZonedDateTime bwcDateValue =
                    AccessController.doPrivileged((PrivilegedAction<JodaCompatibleZonedDateTime>) dates::getDate, noPermissionsAcc);
                assertEquals(expectedDates[d].length > 0 ? expectedDates[d][0] : new DateTime(0, DateTimeZone.UTC), bwcDateValue);
                valuesExist = expectedDates[d].length > 0;
            }

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

    private Dates wrap(long[][] values, BiConsumer<String, String> deprecationHandler) {
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
        }, deprecationHandler);
    }

    public void testGetValues() {
        Set<String> keys = new HashSet<>();
        Set<String> warnings = new HashSet<>();

        Dates dates = biconsumerWrap((deprecationKey, deprecationMessage) -> {
            keys.add(deprecationKey);
            warnings.add(deprecationMessage);

            // Create a temporary directory to prove we are running with the server's permissions.
            createTempDir();
        });

        /*
         * Invoke getValues() without any permissions to verify it still works.
         * This is done using the callback created above, which creates a temp
         * directory, which is not possible with "noPermission".
         */
        PermissionCollection noPermissions = new Permissions();
        AccessControlContext noPermissionsAcc = new AccessControlContext(
            new ProtectionDomain[] {
                new ProtectionDomain(null, noPermissions)
            }
        );

        AccessController.doPrivileged(new PrivilegedAction<Void>(){
            public Void run() {
                dates.getValues();
                return null;
            }
        }, noPermissionsAcc);

        assertThat(warnings, contains(
            "Deprecated getValues used, the field is a list and should be accessed directly."
           + " For example, use doc['foo'] instead of doc['foo'].values."));
        assertThat(keys, contains("ScriptDocValues#getValues"));
    }

    private Dates biconsumerWrap(BiConsumer<String, String> deprecationHandler) {
        return new Dates(new AbstractSortedNumericDocValues() {
            @Override
            public boolean advanceExact(int doc) {
                return true;
            }
            @Override
            public int docValueCount() {
                return 0;
            }
            @Override
            public long nextValue() {
                return 0L;
            }
        }, deprecationHandler);
    }
}
