/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TsidLayoutTests extends ESTestCase {

    private static IndexMetadata indexMetadata(IndexVersion version, @Nullable String tsidDimensionsPrefix) {
        Settings.Builder settings = settings(version).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim.*");
        if (tsidDimensionsPrefix != null) {
            settings.put(IndexSettings.TIME_SERIES_TSID_DIMENSIONS_PREFIX.getKey(), tsidDimensionsPrefix);
        }
        return IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
    }

    public void testForIndexReadsTheSetting() {
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
        assertThat(TsidLayout.forIndex(indexMetadata(version, null)), equalTo(new TsidLayout(version, "resource.attributes.")));
        assertThat(TsidLayout.forIndex(indexMetadata(version, "labels.")), equalTo(new TsidLayout(version, "labels.")));
        assertThat(TsidLayout.forIndex(indexMetadata(version, "")), equalTo(new TsidLayout(version, "")));
    }

    public void testForVersionAssumesDefaultSettings() {
        IndexVersion version = IndexVersionUtils.randomVersion();
        assertThat(TsidLayout.forVersion(version), equalTo(TsidLayout.forIndex(indexMetadata(version, null))));
        assertThat(TsidLayout.forVersion(version).dimensionsPrefix(), equalTo("resource.attributes."));
    }

    public void testEffectiveDimensionsPrefix() {
        IndexVersion current = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
        assertThat(TsidLayout.forVersion(current).effectiveDimensionsPrefix(), equalTo("resource.attributes."));
        assertThat(new TsidLayout(current, "labels.").effectiveDimensionsPrefix(), equalTo("labels."));
        assertThat(new TsidLayout(current, "").effectiveDimensionsPrefix(), nullValue());

        IndexVersion previous = IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
        assertThat(TsidLayout.forVersion(previous).effectiveDimensionsPrefix(), nullValue());
        assertThat(new TsidLayout(previous, "labels.").effectiveDimensionsPrefix(), nullValue());
    }

    public void testSingleBytePrefix() {
        assertTrue(TsidLayout.forVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE).singleBytePrefix());
        assertTrue(TsidLayout.forVersion(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH).singleBytePrefix());
        assertFalse(
            TsidLayout.forVersion(IndexVersionUtils.getPreviousVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG))
                .singleBytePrefix()
        );
    }

    public void testRejectsNulls() {
        expectThrows(NullPointerException.class, () -> new TsidLayout(null, "resource.attributes."));
        expectThrows(NullPointerException.class, () -> new TsidLayout(IndexVersion.current(), null));
    }

    public void testSettingIsFinal() {
        assertTrue(IndexSettings.TIME_SERIES_TSID_DIMENSIONS_PREFIX.isFinal());
    }
}
