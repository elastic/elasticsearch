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

public class ScriptDocValuesDatesTests extends ESTestCase {

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
