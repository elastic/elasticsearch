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

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SkipSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testSkip() {
        SkipSection section = new SkipSection("6.0.0 - 6.1.0",
                randomBoolean() ? Collections.emptyList() : Collections.singletonList("warnings"), "foobar");
        assertFalse(section.skip(Version.CURRENT));
        assertTrue(section.skip(Version.fromString("6.0.0")));
        section = new SkipSection(randomBoolean() ? null : "6.0.0 - 6.1.0",
                Collections.singletonList("boom"), "foobar");
        assertTrue(section.skip(Version.CURRENT));
    }

    public void testMessage() {
        SkipSection section = new SkipSection("6.0.0 - 6.1.0",
                Collections.singletonList("warnings"), "foobar");
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(null, Collections.singletonList("warnings"), "foobar");
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(null, Collections.singletonList("warnings"), null);
        assertEquals("[FOOBAR] skipped, unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
    }

    public void testParseSkipSectionVersionNoFeature() throws Exception {
        Version version = VersionUtils.randomVersion(random());
        parser = createParser(YamlXContent.yamlXContent,
                "version:     \" - " + version + "\"\n" +
                "reason:      Delete ignores the parent param"
        );

        SkipSection skipSection = SkipSection.parse(parser);
        assertThat(skipSection, notNullValue());
        assertThat(skipSection.getLowerVersion(), equalTo(VersionUtils.getFirstVersion()));
        assertThat(skipSection.getUpperVersion(), equalTo(version));
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
