/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.test.index.IndexVersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;

public class HumanReadableIndexSettingsTests extends ESTestCase {
    public void testHumanReadableSettings() {
        IndexVersion versionCreated = randomVersion();
        long created = System.currentTimeMillis();
        Settings testSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, versionCreated)
            .put(IndexMetadata.SETTING_CREATION_DATE, created)
            .build();

        Settings humanSettings = IndexMetadata.addHumanReadableSettings(testSettings);
        assertThat(humanSettings.size(), equalTo(4));
        assertEquals(versionCreated.toReleaseVersion(), humanSettings.get(IndexMetadata.SETTING_VERSION_CREATED_STRING, null));
        ZonedDateTime creationDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC);
        assertEquals(creationDate.toString(), humanSettings.get(IndexMetadata.SETTING_CREATION_DATE_STRING, null));
    }
}
