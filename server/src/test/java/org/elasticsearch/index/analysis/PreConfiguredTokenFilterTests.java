/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

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

        Version version1 = VersionUtils.randomVersion(random());
        Settings settings1 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version1).build();
        TokenFilterFactory tff_v1_1 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings1);
        TokenFilterFactory tff_v1_2 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings1);
        assertSame(tff_v1_1, tff_v1_2);

        Version version2 = randomValueOtherThan(version1, () -> randomFrom(VersionUtils.allVersions()));
        Settings settings2 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build();

        TokenFilterFactory tff_v2 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings2);
        assertSame(tff_v1_1, tff_v2);
    }

    public void testCachingWithElasticsearchVersion() throws IOException {
        PreConfiguredTokenFilter pctf = PreConfiguredTokenFilter.elasticsearchVersion(
            "elasticsearch_version",
            randomBoolean(),
            (tokenStream, esVersion) -> new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            }
        );

        Version version1 = VersionUtils.randomVersion(random());
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

        Version version2 = randomValueOtherThan(version1, () -> randomFrom(VersionUtils.allVersions()));
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

        Version version1 = Version.CURRENT;
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

        byte major = VersionUtils.getFirstVersion().major;
        Version version2 = Version.fromString(major - 1 + ".0.0");
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
