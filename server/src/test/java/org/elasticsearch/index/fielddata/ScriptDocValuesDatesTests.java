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
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ScriptDocValuesDatesTests extends ESTestCase {

    public void testJavaTime() throws IOException {
        assertDateDocValues(true);
    }

    public void testJodaTimeBwc() throws IOException {
        assertDateDocValues(false, "The joda time api for doc values is deprecated." +
            " Use -Des.scripting.use_java_time=true to use the java time api for date field doc values");
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
        Dates dates = wrap(values, deprecationMessage -> {
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

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            dates.setNextDocId(d);
            if (expectedDates[d].length > 0) {
                Object dateValue = AccessController.doPrivileged((PrivilegedAction<Object>) dates::getValue, noPermissionsAcc);
                assertEquals(expectedDates[d][0] , dateValue);
            } else {
                Exception e = expectThrows(IllegalStateException.class, () -> dates.getValue());
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
            }

            assertEquals(values[d].length, dates.size());
            for (int i = 0; i < values[d].length; i++) {
                final int ndx = i;
                Object dateValue = AccessController.doPrivileged((PrivilegedAction<Object>) () -> dates.get(ndx), noPermissionsAcc);
                assertEquals(expectedDates[d][i], dateValue);
            }
        }

        assertThat(warnings, containsInAnyOrder(expectedWarnings));
    }

    private Dates wrap(long[][] values, Consumer<String> deprecationHandler, boolean useJavaTime) {
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
