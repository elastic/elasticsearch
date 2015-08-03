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
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.Version.V_0_20_0;
import static org.elasticsearch.Version.V_0_90_0;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class VersionTests extends ESTestCase {
    
    public void testMavenVersion() {
        // maven sets this property to ensure that the latest version
        // we use here is the version that is actually set to the project.version
        // in maven
        String property = System.getProperty("tests.version", null);
        assumeTrue("tests.version is set", property != null);
        assertEquals(property, Version.CURRENT.toString());
    }
    
    public void testVersionComparison() throws Exception {
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
            if (version.snapshot()) { // number doesn't include SNAPSHOT but the parser checks for that
                assertEquals(Version.fromString(version.number()), version);
            } else {
                assertThat(Version.fromString(version.number()), sameInstance(version));
            }
            assertFalse(Version.fromString(version.number()).snapshot());
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

    @Test(expected = IllegalStateException.class)
    public void testVersionNoPresentInSettings() {
        Version.indexCreated(Settings.builder().build());
    }

    public void testIndexCreatedVersion() {
        // an actual index has a IndexMetaData.SETTING_INDEX_UUID
        final Version version = randomFrom(Version.V_0_18_0, Version.V_0_90_13, Version.V_1_3_0);
        assertEquals(version, Version.indexCreated(Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, "foo").put(IndexMetaData.SETTING_VERSION_CREATED, version).build()));
    }
    
    public void testMinCompatVersion() {
        assertThat(Version.V_2_0_0_beta1.minimumCompatibilityVersion(), equalTo(Version.V_2_0_0_beta1));
        assertThat(Version.V_1_3_0.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0));
        assertThat(Version.V_1_2_0.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0));
        assertThat(Version.V_1_2_3.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0));
        assertThat(Version.V_1_0_0_RC2.minimumCompatibilityVersion(), equalTo(Version.V_1_0_0_RC2));
    }

    public void testToString() {
        // with 2.0.beta we lowercase
        assertEquals("2.0.0-beta1", Version.V_2_0_0_beta1.number());
        assertEquals("1.4.0.Beta1", Version.V_1_4_0_Beta1.number());
        assertEquals("1.4.0", Version.V_1_4_0.number());
    }

    public void testIsBeta() {
        assertTrue(Version.V_2_0_0_beta1.isBeta());
        assertTrue(Version.V_1_4_0_Beta1.isBeta());
        assertFalse(Version.V_1_4_0.isBeta());
    }
    
    public void testParseVersion() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            if (version.snapshot() == false && random().nextBoolean()) {
                version = new Version(version.id, true, version.luceneVersion);
            }
            Version parsedVersion = Version.fromString(version.toString());
            assertEquals(version, parsedVersion);
            assertEquals(version.snapshot(), parsedVersion.snapshot());
        }
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
        for (java.lang.reflect.Field field : Version.class.getDeclaredFields()) {
            if (field.getName().endsWith("_ID")) {
                assertTrue(field.getName() + " should be static", Modifier.isStatic(field.getModifiers()));
                assertTrue(field.getName() + " should be final", Modifier.isFinal(field.getModifiers()));
                int versionId = (Integer)field.get(Version.class);
                
                String constantName = field.getName().substring(0, field.getName().length() - 3);
                java.lang.reflect.Field versionConstant = Version.class.getField(constantName);
                assertTrue(constantName + " should be static", Modifier.isStatic(versionConstant.getModifiers()));
                assertTrue(constantName + " should be final", Modifier.isFinal(versionConstant.getModifiers()));
           
                Version v = (Version) versionConstant.get(Version.class);
                logger.info("Checking " + v);
                assertEquals("Version id " + field.getName() + " does not point to " + constantName, v, Version.fromId(versionId));
                assertEquals("Version " + constantName + " does not have correct id", versionId, v.id);
                if (v.major >= 2) {
                    String number = v.number();
                    if (v.isBeta()) {
                        number = number.replace("-beta", "_beta");
                    } else if (v.isRC()) {
                        number = number.replace("-rc", "_rc");
                    }
                    assertEquals("V_" + number.replace('.', '_'), constantName);
                } else {
                    assertEquals("V_" + v.number().replace('.', '_'), constantName);
                }
                
                // only the latest version for a branch should be a snapshot (ie unreleased)
                String branchName = "" + v.major + "." + v.minor;
                if (v.equals(Version.V_2_0_0_beta1)) {
                    assertTrue("Remove this once beta1 is released", v.snapshot());
                    continue; // this is just a temporary fix until we have a snapshot for the beta since we now have 2 unreleased version of the same major.minor group
                }
                Version maxBranchVersion = maxBranchVersions.get(branchName);
                if (maxBranchVersion == null) {
                    maxBranchVersions.put(branchName, v);
                } else if (v.after(maxBranchVersion)) {

                    assertFalse("Version " + maxBranchVersion + " cannot be a snapshot because version " + v + " exists", maxBranchVersion.snapshot());
                    maxBranchVersions.put(branchName, v);
                }
            }
        }
    }

}
