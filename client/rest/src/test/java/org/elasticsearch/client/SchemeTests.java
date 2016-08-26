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

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests {@link Scheme}.
 */
public class SchemeTests extends RestClientTestCase {

    public void testToString() {
        for (final Scheme scheme : Scheme.values()) {
            assertThat(scheme.toString(), equalTo(scheme.name().toLowerCase(Locale.ROOT)));
        }
    }

    public void testFromString() {
        for (final Scheme scheme : Scheme.values()) {
            assertThat(Scheme.fromString(scheme.name()), sameInstance(scheme));
            assertThat(Scheme.fromString(scheme.name().toLowerCase(Locale.ROOT)), sameInstance(scheme));
        }
    }

    public void testFromStringMalformed() {
        assertIllegalScheme("htp");
        assertIllegalScheme("htttp");
        assertIllegalScheme("httpd");
        assertIllegalScheme("ftp");
        assertIllegalScheme("ws");
        assertIllegalScheme("wss");
        assertIllegalScheme("gopher");
    }

    private void assertIllegalScheme(final String scheme) {
        try {
            Scheme.fromString(scheme);
            fail("scheme should be unknown: [" + scheme + "]");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[" + scheme + "]"));
        }
    }

}
