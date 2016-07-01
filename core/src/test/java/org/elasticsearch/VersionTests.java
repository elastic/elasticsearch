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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.Version.V_2_2_0;
import static org.elasticsearch.Version.V_5_0_0_alpha1;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class VersionTests extends ESTestCase {

    public void testVersionComparison() throws Exception {
        assertThat(V_2_2_0.before(V_5_0_0_alpha1), is(true));
        assertThat(V_2_2_0.before(V_2_2_0), is(false));
        assertThat(V_5_0_0_alpha1.before(V_2_2_0), is(false));

        assertThat(V_2_2_0.onOrBefore(V_5_0_0_alpha1), is(true));
        assertThat(V_2_2_0.onOrBefore(V_2_2_0), is(true));
        assertThat(V_5_0_0_alpha1.onOrBefore(V_2_2_0), is(false));

        assertThat(V_2_2_0.after(V_5_0_0_alpha1), is(false));
        assertThat(V_2_2_0.after(V_2_2_0), is(false));
        assertThat(V_5_0_0_alpha1.after(V_2_2_0), is(true));

        assertThat(V_2_2_0.onOrAfter(V_5_0_0_alpha1), is(false));
        assertThat(V_2_2_0.onOrAfter(V_2_2_0), is(true));
        assertThat(V_5_0_0_alpha1.onOrAfter(V_2_2_0), is(true));

        assertTrue(Version.fromString("5.0.0-alpha2").onOrAfter(Version.fromString("5.0.0-alpha1")));
        assertTrue(Version.fromString("5.0.0").onOrAfter(Version.fromString("5.0.0-beta2")));
        assertTrue(Version.fromString("5.0.0-rc1").onOrAfter(Version.fromString("5.0.0-beta24")));
        assertTrue(Version.fromString("5.0.0-alpha24").before(Version.fromString("5.0.0-beta0")));

    }

    public void testVersionConstantPresent() {
        assertThat(Version.CURRENT, sameInstance(Version.fromId(Version.CURRENT.id)));
        assertThat(Version.CURRENT.luceneVersion, equalTo(org.apache.lucene.util.Version.LATEST));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            assertThat(version, sameInstance(Version.fromId(version.id)));
            assertThat(version.luceneVersion, sameInstance(Version.fromId(version.id).luceneVersion));
        }
    }

    public void testCURRENTIsLatest() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            if (version != Version.CURRENT) {
                assertThat("Version: " + version + " should be before: " + Version.CURRENT + " but wasn't", version.before(Version.CURRENT), is(true));
            }
        }
    }

    public void testVersionFromString() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            assertThat(Version.fromString(version.toString()), sameInstance(version));
        }
    }

    public void testTooLongVersionFromString() {
        try {
            Version.fromString("1.0.0.1.3");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
        }
    }

    public void testTooShortVersionFromString() {
        try {
            Version.fromString("1.0");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
        }

    }

    public void testWrongVersionFromString() {
        try {
            Version.fromString("WRONG.VERSION");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
        }
    }

    public void testVersionNoPresentInSettings() {
        try {
            Version.indexCreated(Settings.builder().build());
            fail("Expected IllegalArgumentException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("[index.version.created] is not present"));
        }
    }

    public void testIndexCreatedVersion() {
        // an actual index has a IndexMetaData.SETTING_INDEX_UUID
        final Version version = randomFrom(Version.V_2_0_0, Version.V_2_3_0, Version.V_5_0_0_alpha1);
        assertEquals(version, Version.indexCreated(Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, "foo").put(IndexMetaData.SETTING_VERSION_CREATED, version).build()));
    }

    public void testMinCompatVersion() {
        assertThat(Version.V_2_0_0_beta1.minimumCompatibilityVersion(), equalTo(Version.V_2_0_0_beta1));
        assertThat(Version.V_2_1_0.minimumCompatibilityVersion(), equalTo(Version.V_2_0_0));
        assertThat(Version.V_2_2_0.minimumCompatibilityVersion(), equalTo(Version.V_2_0_0));
        assertThat(Version.V_2_3_0.minimumCompatibilityVersion(), equalTo(Version.V_2_0_0));
        assertThat(Version.V_5_0_0_alpha1.minimumCompatibilityVersion(), equalTo(Version.V_5_0_0_alpha1));
    }

    public void testToString() {
        // with 2.0.beta we lowercase
        assertEquals("2.0.0-beta1", Version.V_2_0_0_beta1.toString());
        assertEquals("5.0.0-alpha1", Version.V_5_0_0_alpha1.toString());
        assertEquals("2.3.0", Version.V_2_3_0.toString());
        assertEquals("0.90.0.Beta1", Version.fromString("0.90.0.Beta1").toString());
        assertEquals("1.0.0.Beta1", Version.fromString("1.0.0.Beta1").toString());
        assertEquals("2.0.0-beta1", Version.fromString("2.0.0-beta1").toString());
        assertEquals("5.0.0-beta1", Version.fromString("5.0.0-beta1").toString());
        assertEquals("5.0.0-alpha1", Version.fromString("5.0.0-alpha1").toString());
    }

    public void testIsBeta() {
        assertTrue(Version.V_2_0_0_beta1.isBeta());
        assertTrue(Version.fromString("1.0.0.Beta1").isBeta());
        assertTrue(Version.fromString("0.90.0.Beta1").isBeta());
    }


    public void testIsAlpha() {
        assertTrue(new Version(5000001, org.apache.lucene.util.Version.LUCENE_6_0_0).isAlpha());
        assertFalse(new Version(4000002, org.apache.lucene.util.Version.LUCENE_6_0_0).isAlpha());
        assertTrue(new Version(4000002, org.apache.lucene.util.Version.LUCENE_6_0_0).isBeta());
        assertTrue(Version.fromString("5.0.0-alpha14").isAlpha());
        assertEquals(5000014, Version.fromString("5.0.0-alpha14").id);
        assertTrue(Version.fromId(5000015).isAlpha());

        for (int i = 0 ; i < 25; i++) {
            assertEquals(Version.fromString("5.0.0-alpha" + i).id, Version.fromId(5000000 + i).id);
            assertEquals("5.0.0-alpha" + i, Version.fromId(5000000 + i).toString());
        }

        for (int i = 0 ; i < 25; i++) {
            assertEquals(Version.fromString("5.0.0-beta" + i).id, Version.fromId(5000000 + i + 25).id);
            assertEquals("5.0.0-beta" + i, Version.fromId(5000000 + i + 25).toString());
        }
    }


    public void testParseVersion() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            if (random().nextBoolean()) {
                version = new Version(version.id, version.luceneVersion);
            }
            Version parsedVersion = Version.fromString(version.toString());
            assertEquals(version, parsedVersion);
        }

        expectThrows(IllegalArgumentException.class, () -> {
            Version.fromString("5.0.0-alph2");
        });
        assertSame(Version.CURRENT, Version.fromString(Version.CURRENT.toString()));

        assertSame(Version.fromString("2.0.0-SNAPSHOT"), Version.fromString("2.0.0"));

        expectThrows(IllegalArgumentException.class, () -> {
            Version.fromString("5.0.0-SNAPSHOT");
        });
    }

    public void testParseLenient() {
        // note this is just a silly sanity check, we test it in lucene
        for (Version version : VersionUtils.allVersions()) {
            org.apache.lucene.util.Version luceneVersion = version.luceneVersion;
            String string = luceneVersion.toString().toUpperCase(Locale.ROOT)
                    .replaceFirst("^LUCENE_(\\d+)_(\\d+)$", "$1.$2");
            assertThat(luceneVersion, Matchers.equalTo(Lucene.parseVersionLenient(string, null)));
        }
    }

    public void testAllVersionsMatchId() throws Exception {
        Map<String, Version> maxBranchVersions = new HashMap<>();
        for (java.lang.reflect.Field field : Version.class.getFields()) {
            if (field.getName().endsWith("_ID")) {
                assertTrue(field.getName() + " should be static", Modifier.isStatic(field.getModifiers()));
                assertTrue(field.getName() + " should be final", Modifier.isFinal(field.getModifiers()));
                int versionId = (Integer)field.get(Version.class);

                String constantName = field.getName().substring(0, field.getName().length() - 3);
                java.lang.reflect.Field versionConstant = Version.class.getField(constantName);
                assertTrue(constantName + " should be static", Modifier.isStatic(versionConstant.getModifiers()));
                assertTrue(constantName + " should be final", Modifier.isFinal(versionConstant.getModifiers()));

                Version v = (Version) versionConstant.get(Version.class);
                logger.info("Checking {}", v);
                assertEquals("Version id " + field.getName() + " does not point to " + constantName, v, Version.fromId(versionId));
                assertEquals("Version " + constantName + " does not have correct id", versionId, v.id);
                if (v.major >= 2) {
                    String number = v.toString();
                    if (v.isBeta()) {
                        number = number.replace("-beta", "_beta");
                    } else if (v.isRC()) {
                        number = number.replace("-rc", "_rc");
                    } else if (v.isAlpha()) {
                        number = number.replace("-alpha", "_alpha");
                    }
                    assertEquals("V_" + number.replace('.', '_'), constantName);
                } else {
                    assertEquals("V_" + v.toString().replace('.', '_'), constantName);
                }

                // only the latest version for a branch should be a snapshot (ie unreleased)
                String branchName = "" + v.major + "." + v.minor;
                Version maxBranchVersion = maxBranchVersions.get(branchName);
                if (maxBranchVersion == null) {
                    maxBranchVersions.put(branchName, v);
                } else if (v.after(maxBranchVersion)) {

                    assertFalse("Version " + maxBranchVersion + " cannot be a snapshot because version " + v + " exists", VersionUtils.isSnapshot(maxBranchVersion));
                    maxBranchVersions.put(branchName, v);
                }
            }
        }
    }

    // this test ensures we never bump the lucene version in a bugfix release
    public void testLuceneVersionIsSameOnMinorRelease() {
        for (Version version : VersionUtils.allVersions()) {
            for (Version other : VersionUtils.allVersions()) {
                if (other.onOrAfter(version)) {
                    assertTrue("lucene versions must be "  + other + " >= " + version,
                        other.luceneVersion.onOrAfter(version.luceneVersion));
                }
                if (other.isAlpha() == false && version.isAlpha() == false
                        && other.major == version.major && other.minor == version.minor) {
                    assertEquals(other.luceneVersion.major, version.luceneVersion.major);
                    assertEquals(other.luceneVersion.minor, version.luceneVersion.minor);
                    // should we also assert the lucene bugfix version?
                }
            }
        }
    }
}
