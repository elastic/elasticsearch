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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.elasticsearch.test.VersionUtils.randomVersion;

public class HumanReadableIndexSettingsTests extends ESTestCase {

    @Test
    public void testHumanReadableSettings() {
        Version versionCreated = randomVersion(random());
        Version versionUpgraded = randomVersion(random());
        long created = System.currentTimeMillis();
        Settings testSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, versionCreated)
                .put(IndexMetaData.SETTING_VERSION_UPGRADED, versionUpgraded)
                .put(IndexMetaData.SETTING_CREATION_DATE, created)
                .build();

        Settings humanSettings = IndexMetaData.addHumanReadableSettings(testSettings);

        assertEquals(versionCreated.toString(), humanSettings.get(IndexMetaData.SETTING_VERSION_CREATED_STRING, null));
        assertEquals(versionUpgraded.toString(), humanSettings.get(IndexMetaData.SETTING_VERSION_UPGRADED_STRING, null));
        assertEquals(new DateTime(created, DateTimeZone.UTC).toString(), humanSettings.get(IndexMetaData.SETTING_CREATION_DATE_STRING, null));
    }
}
