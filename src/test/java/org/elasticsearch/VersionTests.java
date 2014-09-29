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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Locale;

import static org.elasticsearch.Version.V_0_20_0;
import static org.elasticsearch.Version.V_0_90_0;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class VersionTests extends ElasticsearchTestCase {

    @Test
    public void testMavenVersion() {
        // maven sets this property to ensure that the latest version
        // we use here is the version that is actually set to the project.version
        // in maven
        String property = System.getProperty("tests.version", null);
        assumeNotNull(property);
        assertEquals(property, Version.CURRENT.toString());
    }

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
        assertThat(Version.CURRENT.luceneVersion, equalTo(org.apache.lucene.util.Version.LATEST));
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

    public void testVersion() {
        // test scenario
        assertEquals(Version.CURRENT, Version.indexCreated(ImmutableSettings.builder().build()));
        // an actual index has a IndexMetaData.SETTING_UUID
        final Version version = randomFrom(Version.V_0_18_0, Version.V_0_90_13, Version.V_1_3_0);
        assertEquals(version, Version.indexCreated(ImmutableSettings.builder().put(IndexMetaData.SETTING_UUID, "foo").put(IndexMetaData.SETTING_VERSION_CREATED, version).build()));
    }

    @Test
    public void testMinCompatVersion() {
        assertThat(Version.V_2_0_0.minimumCompatibilityVersion(), equalTo(Version.V_2_0_0));
        assertThat(Version.V_1_3_0.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0));
        assertThat(Version.V_1_2_0.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0));
        assertThat(Version.V_1_2_3.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0));
        assertThat(Version.V_1_0_0_RC2.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0_RC2));
    }

    @Test
    public void parseLenient() {
        // note this is just a silly sanity check, we test it in lucene
        for (Version version : allVersions()) {
            org.apache.lucene.util.Version luceneVersion = version.luceneVersion;
            String string = luceneVersion.toString().toUpperCase(Locale.ROOT)
                    .replaceFirst("^LUCENE_(\\d+)_(\\d+)$", "$1.$2");
            assertThat(luceneVersion, Matchers.equalTo(Lucene.parseVersionLenient(string, null)));
        }
    }

}
