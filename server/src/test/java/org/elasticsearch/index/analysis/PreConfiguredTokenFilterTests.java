/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;

public class PreConfiguredTokenFilterTests extends ESTestCase {

    private final Settings emptyNodeSettings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    public void testCachingWithSingleton() throws IOException {
        PreConfiguredTokenFilter pctf = PreConfiguredTokenFilter.singleton(
            "singleton",
            randomBoolean(),
            (tokenStream) -> new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            }
        );

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        IndexVersion version1 = IndexVersionUtils.randomVersion();
        Settings settings1 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version1).build();
        TokenFilterFactory tff_v1_1 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings1);
        TokenFilterFactory tff_v1_2 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings1);
        assertSame(tff_v1_1, tff_v1_2);

        IndexVersion version2 = randomValueOtherThan(version1, () -> randomFrom(IndexVersionUtils.allReleasedVersions()));
        Settings settings2 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build();

        TokenFilterFactory tff_v2 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings2);
        assertSame(tff_v1_1, tff_v2);
    }

    public void testCachingWithElasticsearchVersion() throws IOException {
        PreConfiguredTokenFilter pctf = PreConfiguredTokenFilter.indexVersion(
            "elasticsearch_version",
            randomBoolean(),
            (tokenStream, esVersion) -> new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            }
        );

        IndexVersion version1 = IndexVersionUtils.randomVersion();
        IndexSettings indexSettings1 = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version1).build()
        );
        TokenFilterFactory tff_v1_1 = pctf.get(
            indexSettings1,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "elasticsearch_version",
            Settings.EMPTY
        );
        TokenFilterFactory tff_v1_2 = pctf.get(
            indexSettings1,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "elasticsearch_version",
            Settings.EMPTY
        );
        assertSame(tff_v1_1, tff_v1_2);

        IndexVersion version2 = randomValueOtherThan(version1, () -> randomFrom(IndexVersionUtils.allReleasedVersions()));
        IndexSettings indexSettings2 = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build()
        );
        Settings settings2 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build();

        TokenFilterFactory tff_v2 = pctf.get(
            indexSettings2,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "elasticsearch_version",
            settings2
        );
        assertNotSame(tff_v1_1, tff_v2);
    }

    public void testCachingWithLuceneVersion() throws IOException {
        PreConfiguredTokenFilter pctf = PreConfiguredTokenFilter.luceneVersion(
            "lucene_version",
            randomBoolean(),
            (tokenStream, luceneVersion) -> new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            }
        );

        IndexVersion version1 = IndexVersion.current();
        IndexSettings indexSettings1 = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version1).build()
        );

        TokenFilterFactory tff_v1_1 = pctf.get(
            indexSettings1,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "lucene_version",
            Settings.EMPTY
        );
        TokenFilterFactory tff_v1_2 = pctf.get(
            indexSettings1,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "lucene_version",
            Settings.EMPTY
        );
        assertSame(tff_v1_1, tff_v1_2);

        IndexVersion version2 = IndexVersionUtils.getPreviousMajorVersion(IndexVersionUtils.getLowestReadCompatibleVersion());
        IndexSettings indexSettings2 = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build()
        );

        TokenFilterFactory tff_v2 = pctf.get(
            indexSettings2,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "lucene_version",
            Settings.EMPTY
        );
        assertNotSame(tff_v1_1, tff_v2);
    }
}
