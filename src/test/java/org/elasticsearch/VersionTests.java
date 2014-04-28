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

package org.elasticsearch;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.Version.V_0_20_0;
import static org.elasticsearch.Version.V_0_90_0;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class VersionTests extends ElasticsearchTestCase {

    @Test
    public void testVersions() throws Exception {
        assertThat(V_0_20_0.before(V_0_90_0), is(true));
        assertThat(V_0_20_0.before(V_0_20_0), is(false));
        assertThat(V_0_90_0.before(V_0_20_0), is(false));

        assertThat(V_0_20_0.onOrBefore(V_0_90_0), is(true));
        assertThat(V_0_20_0.onOrBefore(V_0_20_0), is(true));
        assertThat(V_0_90_0.onOrBefore(V_0_20_0), is(false));

        assertThat(V_0_20_0.after(V_0_90_0), is(false));
        assertThat(V_0_20_0.after(V_0_20_0), is(false));
        assertThat(V_0_90_0.after(V_0_20_0), is(true));

        assertThat(V_0_20_0.onOrAfter(V_0_90_0), is(false));
        assertThat(V_0_20_0.onOrAfter(V_0_20_0), is(true));
        assertThat(V_0_90_0.onOrAfter(V_0_20_0), is(true));
    }

    @Test
    public void testVersionConstantPresent() {
        assertThat(Version.CURRENT, sameInstance(Version.fromId(Version.CURRENT.id)));
        assertThat(Version.CURRENT.luceneVersion.ordinal(), equalTo(org.apache.lucene.util.Version.LUCENE_CURRENT.ordinal() - 1));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion();
            assertThat(version, sameInstance(Version.fromId(version.id)));
            assertThat(version.luceneVersion, sameInstance(Version.fromId(version.id).luceneVersion));
        }
    }
    @Test
    public void testCURRENTIsLatest() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion();
            if (version != Version.CURRENT) {
                assertThat("Version: " + version + " should be before: " + Version.CURRENT + " but wasn't", version.before(Version.CURRENT), is(true));
            }
        }
    }

    @Test
    public void testVersionFromString() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion();
            assertThat(Version.fromString(version.number()), sameInstance(version));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLongVersionFromString() {
        Version.fromString("1.0.0.1.3");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooShortVersionFromString() {
        Version.fromString("1.0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongVersionFromString() {
        Version.fromString("WRONG.VERSION");
    }
}