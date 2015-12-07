/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.marvel.support.VersionUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.MARVEL_VERSION_FIELD;

public class MarvelTemplateUtilsTests extends ESTestCase {

    public void testLoadTemplate() {
        byte[] template = MarvelTemplateUtils.loadDefaultTemplate();
        assertNotNull(template);
        assertThat(template.length, Matchers.greaterThan(0));
    }

    public void testParseTemplateVersionFromByteArrayTemplate() throws IOException {
        byte[] template = MarvelTemplateUtils.loadDefaultTemplate();
        assertNotNull(template);

        Version version = MarvelTemplateUtils.parseTemplateVersion(template);
        assertNotNull(version);
    }

    public void testParseTemplateVersionFromStringTemplate() throws IOException {
        List<String> templates = new ArrayList<>();
        templates.add("{\"marvel_version\": \"1.4.0.Beta1\"}");
        templates.add("{\"marvel_version\": \"1.6.2-SNAPSHOT\"}");
        templates.add("{\"marvel_version\": \"1.7.1\"}");
        templates.add("{\"marvel_version\": \"2.0.0-beta1\"}");
        templates.add("{\"marvel_version\": \"2.0.0\"}");
        templates.add("{  \"template\": \".marvel*\",  \"settings\": {    \"marvel_version\": \"2.0.0-beta1-SNAPSHOT\", \"index.number_of_shards\": 1 } }");

        for (String template : templates) {
            Version version = MarvelTemplateUtils.parseTemplateVersion(Strings.toUTF8Bytes(template));
            assertNotNull(version);
        }

        Version version = MarvelTemplateUtils.parseTemplateVersion(Strings.toUTF8Bytes("{\"marvel.index_format\": \"7\"}"));
        assertNull(version);
    }

    public void testParseVersion() throws IOException {
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel_version\": \"2.0.0-beta1\"}"));
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel_version\": \"2.0.0\"}"));
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel_version\": \"1.5.2\"}"));
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{  \"template\": \".marvel*\",  \"settings\": {    \"marvel_version\": \"2.0.0-beta1-SNAPSHOT\", \"index.number_of_shards\": 1 } }"));
        assertNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel.index_format\": \"7\"}"));
        assertNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD + "unkown", "{\"marvel_version\": \"1.5.2\"}"));
    }

    public void testTemplateVersionMandatesAnUpdate() {
        // Version is unknown
        assertTrue(MarvelTemplateUtils.installedTemplateVersionMandatesAnUpdate(Version.CURRENT, null, logger, "unit-test"));

        // Version is too old
        Version unsupported = org.elasticsearch.test.VersionUtils.getPreviousVersion(MarvelTemplateUtils.MIN_SUPPORTED_TEMPLATE_VERSION);
        assertFalse(MarvelTemplateUtils.installedTemplateVersionMandatesAnUpdate(Version.CURRENT, unsupported, logger, "unit-test"));

        // Version is old but supported
        assertTrue(MarvelTemplateUtils.installedTemplateVersionMandatesAnUpdate(Version.CURRENT, MarvelTemplateUtils.MIN_SUPPORTED_TEMPLATE_VERSION, logger, "unit-test"));

        // Version is up to date
        assertFalse(MarvelTemplateUtils.installedTemplateVersionMandatesAnUpdate(Version.CURRENT, Version.CURRENT, logger, "unit-test"));

        // Version is up to date
        Version previous = org.elasticsearch.test.VersionUtils.getPreviousVersion(Version.CURRENT);
        assertFalse(MarvelTemplateUtils.installedTemplateVersionMandatesAnUpdate(previous, Version.CURRENT, logger, "unit-test"));
    }

    public void testTemplateVersionIsSufficient() {
        // Version is unknown
        assertFalse(MarvelTemplateUtils.installedTemplateVersionIsSufficient(null));

        // Version is too old
        Version unsupported = org.elasticsearch.test.VersionUtils.getPreviousVersion(MarvelTemplateUtils.MIN_SUPPORTED_TEMPLATE_VERSION);
        assertFalse(MarvelTemplateUtils.installedTemplateVersionIsSufficient(unsupported));

        // Version is OK
        assertTrue(MarvelTemplateUtils.installedTemplateVersionIsSufficient(Version.CURRENT));
    }
}
