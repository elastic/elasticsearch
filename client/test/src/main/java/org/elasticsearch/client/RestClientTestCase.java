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
import org.apache.http.message.BasicHeader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Create the specified number of {@link Header}s.
     * <p>
     * Generated header names will be the {@code baseName} plus its index or, rarely, the {@code arrayName} if it's supplied.
     *
     * @param baseName The base name to use for all headers.
     * @param arrayName The optional ({@code null}able) array name to use randomly.
     * @param headers The number of headers to create.
     * @return Never {@code null}.
     */
    protected static Header[] generateHeaders(final String baseName, final String arrayName, final int headers) {
        final Header[] generated = new Header[headers];
        for (int i = 0; i < headers; i++) {
            String headerName = baseName + i;
            if (arrayName != null && rarely()) {
                headerName = arrayName;
            }

            generated[i] = new BasicHeader(headerName, randomAsciiOfLengthBetween(3, 10));
        }
        return generated;
    }

    /**
     * Create a new {@link List} within the {@code map} if none exists for {@code name} or append to the existing list.
     *
     * @param map The map to manipulate.
     * @param name The name to create/append the list for.
     * @param value The value to add.
     */
    private static void createOrAppendList(final Map<String, List<String>> map, final String name, final String value) {
        List<String> values = map.get(name);

        if (values == null) {
            values = new ArrayList<>();
            map.put(name, values);
        }

        values.add(value);
    }

    /**
     * Add the {@code headers} to the {@code map} so that related tests can more easily assert that they exist.
     * <p>
     * If both the {@code defaultHeaders} and {@code headers} contain the same {@link Header}, based on its
     * {@linkplain Header#getName() name}, then this will only use the {@code Header}(s) from {@code headers}.
     *
     * @param map The map to build with name/value(s) pairs.
     * @param defaultHeaders The headers to add to the map representing default headers.
     * @param headers The headers to add to the map representing request-level headers.
     * @see #createOrAppendList(Map, String, String)
     */
    protected static void addHeaders(final Map<String, List<String>> map, final Header[] defaultHeaders, final Header[] headers) {
        final Set<String> uniqueHeaders = new HashSet<>();
        for (final Header header : headers) {
            final String name = header.getName();
            createOrAppendList(map, name, header.getValue());
            uniqueHeaders.add(name);
        }
        for (final Header defaultHeader : defaultHeaders) {
            final String name = defaultHeader.getName();
            if (uniqueHeaders.contains(name) == false) {
                createOrAppendList(map, name, defaultHeader.getValue());
            }
        }
    }

}
