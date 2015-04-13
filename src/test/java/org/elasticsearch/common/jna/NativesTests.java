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

package org.elasticsearch.common.jna;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.jna.Kernel32Library.ConsoleCtrlHandler;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class NativesTests extends ElasticsearchTestCase {

    /**
     * Those properties are set by the JNA Api and if not ignored,
     * lead to tests failure (see AbstractRandomizedTest#IGNORED_INVARIANT_PROPERTIES)
     */
    private static final String[] JNA_INVARIANT_PROPERTIES = {
            "jna.platform.library.path",
            "jnidispatch.path"
    };

    private Map<String, String> properties = new HashMap<>();

    @Before
    public void saveProperties() {
        assumeTrue("Natives can't load libraries from path if security manager is enabled.", System.getSecurityManager() == null);
        for (String p : JNA_INVARIANT_PROPERTIES) {
            properties.put(p, System.getProperty(p));
        }
    }

    @After
    public void restoreProperties() {
        for (String p : JNA_INVARIANT_PROPERTIES) {
            if (properties.get(p) != null) {
                System.setProperty(p, properties.get(p));
            } else {
                System.clearProperty(p);
            }
        }
    }

    @Test
    public void testTryMlockall() {
        Natives.tryMlockall();

        if (Constants.WINDOWS) {
            assertFalse("Memory locking is not available on Windows platforms", Natives.LOCAL_MLOCKALL);
        }
    }

    @Test
    public void testAddConsoleCtrlHandler() {
        ConsoleCtrlHandler handler = new ConsoleCtrlHandler() {
            @Override
            public boolean handle(int code) {
                return false;
            }
        };

        Natives.addConsoleCtrlHandler(handler);

        if (Constants.WINDOWS) {
            assertNotNull(Kernel32Library.getInstance());
            assertThat(Kernel32Library.getInstance().getCallbacks().size(), equalTo(1));

        } else {
            assertNotNull(Kernel32Library.getInstance());
            assertThat(Kernel32Library.getInstance().getCallbacks().size(), equalTo(0));

            try {
                Kernel32Library.getInstance().addConsoleCtrlHandler(handler);
                fail("should have thrown an unsupported operation exception");
            } catch (UnsatisfiedLinkError e) {
                // UnsatisfiedLinkError is expected
            }
        }
    }
}
