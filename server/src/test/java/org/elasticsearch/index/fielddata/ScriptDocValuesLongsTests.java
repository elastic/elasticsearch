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

import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.contains;

public class ScriptDocValuesLongsTests extends ESTestCase {
    public void testLongs() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomLong();
            }
        }

        Set<String> warnings = new HashSet<>();
        Set<String> keys = new HashSet<>();

        Longs longs = wrap(values, (deprecationKey, deprecationMessage) -> {
            keys.add(deprecationKey);
            warnings.add(deprecationMessage);
            
            // Create a temporary directory to prove we are running with the server's permissions.
            createTempDir();
        });

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            longs.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], longs.getValue());
            } else {
                Exception e = expectThrows(IllegalStateException.class, () -> longs.getValue());
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
            }
            assertEquals(values[d].length, longs.size());
            assertEquals(values[d].length, longs.getValues().size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], longs.get(i).longValue());
                assertEquals(values[d][i], longs.getValues().get(i).longValue());
            }

            Exception e = expectThrows(UnsupportedOperationException.class, () -> longs.getValues().add(100L));
            assertEquals("doc values are unmodifiable", e.getMessage());
        }

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
                longs.getValues();
                return null;
            }
        }, noPermissionsAcc);

        assertThat(warnings, contains(
            "Deprecated getValues used, the field is a list and should be accessed directly."
            + " For example, use doc['foo'] instead of doc['foo'].values."));
        assertThat(keys, contains("ScriptDocValues#getValues"));

    }

    private Longs wrap(long[][] values, BiConsumer<String, String> deprecationCallback) {
        return new Longs(new AbstractSortedNumericDocValues() {
            long[] current;
            int i;

            @Override
            public boolean advanceExact(int doc) {
                i = 0;
                current = values[doc];
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
        }, deprecationCallback);
    }
}
