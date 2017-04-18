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

package org.elasticsearch.client;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.http.Header;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@TestMethodProviders({
        JUnit3MethodProvider.class
})
@SeedDecorators({MixWithSuiteName.class}) // See LUCENE-3995 for rationale.
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
@ThreadLeakGroup(ThreadLeakGroup.Group.MAIN)
@ThreadLeakAction({ThreadLeakAction.Action.WARN, ThreadLeakAction.Action.INTERRUPT})
@ThreadLeakZombies(ThreadLeakZombies.Consequence.IGNORE_REMAINING_TESTS)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@TimeoutSuite(millis = 2 * 60 * 60 * 1000)
public abstract class RestClientTestCase extends RandomizedTest {

    /**
     * Assert that the actual headers are the expected ones given the original default and request headers. Some headers can be ignored,
     * for instance in case the http client is adding its own automatically.
     *
     * @param defaultHeaders the default headers set to the REST client instance
     * @param requestHeaders the request headers sent with a particular request
     * @param actualHeaders the actual headers as a result of the provided default and request headers
     * @param ignoreHeaders header keys to be ignored as they are not part of default nor request headers, yet they
     *                      will be part of the actual ones
     */
    protected static void assertHeaders(final Header[] defaultHeaders, final Header[] requestHeaders,
                                        final Header[] actualHeaders, final Set<String> ignoreHeaders) {
        final Map<String, List<String>> expectedHeaders = new HashMap<>();
        final Set<String> requestHeaderKeys = new HashSet<>();
        for (final Header header : requestHeaders) {
            final String name = header.getName();
            addValueToListEntry(expectedHeaders, name, header.getValue());
            requestHeaderKeys.add(name);
        }
        for (final Header defaultHeader : defaultHeaders) {
            final String name = defaultHeader.getName();
            if (requestHeaderKeys.contains(name) == false) {
                addValueToListEntry(expectedHeaders, name, defaultHeader.getValue());
            }
        }
        Set<String> actualIgnoredHeaders = new HashSet<>();
        for (Header responseHeader : actualHeaders) {
            final String name = responseHeader.getName();
            if (ignoreHeaders.contains(name)) {
                expectedHeaders.remove(name);
                actualIgnoredHeaders.add(name);
                continue;
            }
            final String value = responseHeader.getValue();
            final List<String> values = expectedHeaders.get(name);
            assertNotNull("found response header [" + name + "] that wasn't originally sent: " + value, values);
            assertTrue("found incorrect response header [" + name + "]: " + value, values.remove(value));
            if (values.isEmpty()) {
                expectedHeaders.remove(name);
            }
        }
        assertEquals("some headers meant to be ignored were not part of the actual headers", ignoreHeaders, actualIgnoredHeaders);
        assertTrue("some headers that were sent weren't returned " + expectedHeaders, expectedHeaders.isEmpty());
    }

    private static void addValueToListEntry(final Map<String, List<String>> map, final String name, final String value) {
        List<String> values = map.get(name);
        if (values == null) {
            values = new ArrayList<>();
            map.put(name, values);
        }
        values.add(value);
    }
}
