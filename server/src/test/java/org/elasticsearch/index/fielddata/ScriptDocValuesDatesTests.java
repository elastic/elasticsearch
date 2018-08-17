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
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ScriptDocValuesDatesTests extends ESTestCase {
    public void test() throws IOException {
        long[][] values = new long[between(3, 10)][];
        ReadableDateTime[][] expectedDates = new ReadableDateTime[values.length][];
        for (int d = 0; d < values.length; d++) {
            switch (d) {
            case 0:
                // empty
                values[d] = new long[0];
                break;
            case 1:
                // single value
                values[d] = new long[1];
                break;
            case 2:
                // multivalued
                values[d] = new long[between(2, 100)];
                break;
            default:
                // random
                values[d] = new long[between(0, 5)];
                break;
            }
        }
        Collections.shuffle(Arrays.asList(values), random());

        for (int d = 0; d < values.length; d++) {
            expectedDates[d] = new ReadableDateTime[values[d].length];
            for (int i = 0; i < values[d].length; i++) {
                expectedDates[d][i] = new DateTime(randomNonNegativeLong(), DateTimeZone.UTC);
                values[d][i] = expectedDates[d][i].getMillis();
            }
        }
        Set<String> warnings = new HashSet<>();
        Dates dates = wrap(values, deprecationMessage -> {
            warnings.add(deprecationMessage);
            /* Create a temporary directory to prove we are running with the
             * server's permissions. */
            createTempDir();
        });

        for (int d = 0; d < values.length; d++) {
            dates.setNextDocId(d);
            if (expectedDates[d].length > 0) {
                assertEquals(expectedDates[d][0] , dates.getValue());
                assertEquals(expectedDates[d][0] , dates.getDate());
            } else if (ScriptModule.EXCEPTION_FOR_MISSING_VALUE) {
                Exception e = expectThrows(IllegalStateException.class, () -> dates.getValue());
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
            } else {
                assertEquals(0, dates.getValue().getMillis()); // Epoch
            }
            assertEquals(values[d].length, dates.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(expectedDates[d][i], dates.get(i));
            }

            Exception e = expectThrows(UnsupportedOperationException.class, () -> dates.add(new DateTime()));
            assertEquals("doc values are unmodifiable", e.getMessage());
        }

        /*
         * Invoke getDates without any privileges to verify that
         * it still works without any. In particularly, this
         * verifies that the callback that we've configured
         * above works. That callback creates a temporary
         * directory which is not possible with "noPermissions".
         */
        PermissionCollection noPermissions = new Permissions();
        AccessControlContext noPermissionsAcc = new AccessControlContext(
            new ProtectionDomain[] {
                new ProtectionDomain(null, noPermissions)
            }
        );
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                dates.getDates();
                return null;
            }
        }, noPermissionsAcc);

        assertThat(warnings, containsInAnyOrder(
            "getDate is no longer necessary on date fields as the value is now a date.",
            "getDates is no longer necessary on date fields as the values are now dates."));
    }

    private Dates wrap(long[][] values, Consumer<String> deprecationHandler) {
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
}
