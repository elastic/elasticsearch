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

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.VersionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SkipSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testSkip() {
        SkipSection section = new SkipSection("5.0.0 - 5.1.0",
                randomBoolean() ? Collections.emptyList() : Collections.singletonList("warnings"), "foobar");
        assertFalse(section.skip(Version.CURRENT));
        assertTrue(section.skip(Version.V_5_0_0));
        section = new SkipSection(randomBoolean() ? null : "5.0.0 - 5.1.0",
                Collections.singletonList("boom"), "foobar");
        assertTrue(section.skip(Version.CURRENT));
    }

    public void testParseVersionRange() throws Exception {
        List<String> features = Collections.emptyList();
        Arrays.asList("5.0.0 - 5.1.1", "5.0.0-5.1.1", "5.0.0- 5.1.1", "5.0.0 -5.1.1").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_5_0_0, Version.V_5_1_1));
        Arrays.asList("5.0.0 - ", "5.0.0- ", "5.0.0 -", "5.0.0-").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_5_0_0, Version.CURRENT));
        Arrays.asList(" - 5.0.0", " -5.0.0", "-5.0.0", "- 5.0.0").forEach(
            vr -> verifyVersionRange(vr, features, VersionUtils.getFirstVersion(), Version.V_5_0_0));
        Arrays.asList("5.0.0 - 6.0.0-rc1", "5.0.0 -6.0.0-rc1", "5.0.0- 6.0.0-rc1", "5.0.0-6.0.0-rc1").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_5_0_0, Version.V_6_0_0_rc1));
        Arrays.asList("6.0.0-rc1 - 5.0.0", "6.0.0-rc1- 5.0.0", "6.0.0-rc1 -5.0.0", "6.0.0-rc1-5.0.0").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_6_0_0_rc1, Version.V_5_0_0));
        Arrays.asList("5.0.0 - 6.0.0-beta1", "5.0.0 -6.0.0-beta1", "5.0.0- 6.0.0-beta1", "5.0.0-6.0.0-beta1").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_5_0_0, Version.V_6_0_0_beta1));
        Arrays.asList("6.0.0-beta1 - 5.0.0", "6.0.0-beta1- 5.0.0", "6.0.0-beta1 -5.0.0", "6.0.0-beta1-5.0.0").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_6_0_0_beta1, Version.V_5_0_0));
        Arrays.asList("5.0.0 - 6.0.0-alpha1", "5.0.0 -6.0.0-alpha1", "5.0.0- 6.0.0-alpha1", "5.0.0-6.0.0-alpha1").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_5_0_0, Version.V_6_0_0_alpha1));
        Arrays.asList("6.0.0-alpha1 - 5.0.0", "6.0.0-alpha1- 5.0.0", "6.0.0-alpha1 -5.0.0", "6.0.0-alpha1-5.0.0").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_6_0_0_alpha1, Version.V_5_0_0));
        Arrays.asList(" - 6.0.0-rc1", " -6.0.0-rc1", "- 6.0.0-rc1", "-6.0.0-rc1").forEach(
            vr -> verifyVersionRange(vr, features, VersionUtils.getFirstVersion(), Version.V_6_0_0_rc1));
        Arrays.asList("6.0.0-rc1 - ", "6.0.0-rc1- ", "6.0.0-rc1 -", "6.0.0-rc1-").forEach(
            vr -> verifyVersionRange(vr, features, Version.V_6_0_0_rc1, Version.CURRENT));
    }

    private void verifyVersionRange(String versionRange, List<String> features, Version lowerVersion, Version upperVersion) {
        SkipSection skipSection = new SkipSection(versionRange, features, "foo");
        assertEquals(lowerVersion, skipSection.getLowerVersion());
        assertEquals(upperVersion, skipSection.getUpperVersion());
    }

    public void testMessage() {
        SkipSection section = new SkipSection("5.0.0 - 5.1.0",
                Collections.singletonList("warnings"), "foobar");
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(null, Collections.singletonList("warnings"), "foobar");
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(null, Collections.singletonList("warnings"), null);
        assertEquals("[FOOBAR] skipped, unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
    }

    public void testParseSkipSectionVersionNoFeature() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "version:     \" - 5.1.1\"\n" +
                "reason:      Delete ignores the parent param"
        );

        SkipSection skipSection = SkipSection.parse(parser);
        assertThat(skipSection, notNullValue());
        assertThat(skipSection.getLowerVersion(), equalTo(VersionUtils.getFirstVersion()));
        assertThat(skipSection.getUpperVersion(), equalTo(Version.V_5_1_1));
        assertThat(skipSection.getFeatures().size(), equalTo(0));
        assertThat(skipSection.getReason(), equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionAllVersions() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "version:     \" all \"\n" +
            "reason:      Delete ignores the parent param"
        );

        SkipSection skipSection = SkipSection.parse(parser);
        assertThat(skipSection, notNullValue());
        assertThat(skipSection.getLowerVersion(), equalTo(VersionUtils.getFirstVersion()));
        assertThat(skipSection.getUpperVersion(), equalTo(Version.CURRENT));
        assertThat(skipSection.getFeatures().size(), equalTo(0));
        assertThat(skipSection.getReason(), equalTo("Delete ignores the parent param"));
    }

    public void testParseSkipSectionFeatureNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "features:     regex"
        );

        SkipSection skipSection = SkipSection.parse(parser);
        assertThat(skipSection, notNullValue());
        assertThat(skipSection.isVersionCheck(), equalTo(false));
        assertThat(skipSection.getFeatures().size(), equalTo(1));
        assertThat(skipSection.getFeatures().get(0), equalTo("regex"));
        assertThat(skipSection.getReason(), nullValue());
    }

    public void testParseSkipSectionFeaturesNoVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "features:     [regex1,regex2,regex3]"
        );

        SkipSection skipSection = SkipSection.parse(parser);
        assertThat(skipSection, notNullValue());
        assertThat(skipSection.isVersionCheck(), equalTo(false));
        assertThat(skipSection.getFeatures().size(), equalTo(3));
        assertThat(skipSection.getFeatures().get(0), equalTo("regex1"));
        assertThat(skipSection.getFeatures().get(1), equalTo("regex2"));
        assertThat(skipSection.getFeatures().get(2), equalTo("regex3"));
        assertThat(skipSection.getReason(), nullValue());
    }

    public void testParseSkipSectionBothFeatureAndVersion() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "version:     \" - 0.90.2\"\n" +
                "features:     regex\n" +
                "reason:      Delete ignores the parent param"
        );

        SkipSection skipSection = SkipSection.parse(parser);
        assertEquals(VersionUtils.getFirstVersion(), skipSection.getLowerVersion());
        assertEquals(Version.fromString("0.90.2"), skipSection.getUpperVersion());
        assertEquals(Collections.singletonList("regex"), skipSection.getFeatures());
        assertEquals("Delete ignores the parent param", skipSection.getReason());
    }

    public void testParseSkipSectionNoReason() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "version:     \" - 0.90.2\"\n"
        );

        Exception e = expectThrows(ParsingException.class, () -> SkipSection.parse(parser));
        assertThat(e.getMessage(), is("reason is mandatory within skip version section"));
    }

    public void testParseSkipSectionNoVersionNorFeature() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "reason:      Delete ignores the parent param\n"
        );

        Exception e = expectThrows(ParsingException.class, () -> SkipSection.parse(parser));
        assertThat(e.getMessage(), is("version or features is mandatory within skip section"));
    }
}
