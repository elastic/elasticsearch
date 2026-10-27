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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import static org.hamcrest.Matchers.equalTo;

public class TsidLayoutTests extends ESTestCase {

    private static IndexMetadata indexMetadata(IndexVersion version, Settings extra) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), version)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim.*")
            .put(extra)
            .build();
        return IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
    }

    public void testMultiBytePrefixBeforeSinglePrefixByte() {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TSID_CREATED_DURING_ROUTING,
            IndexVersionUtils.getPreviousVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)
        );
        assertThat(TsidLayout.forIndex(indexMetadata(version, Settings.EMPTY)).kind(), equalTo(TsidLayout.Kind.MULTI_BYTE_PREFIX));
        // the setting has no effect on older indices
        Settings explicit = Settings.builder().put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "dim.host").build();
        assertThat(TsidLayout.forIndex(indexMetadata(version, explicit)).kind(), equalTo(TsidLayout.Kind.MULTI_BYTE_PREFIX));
    }

    public void testSinglePrefixByteBeforeDimensionGroupSort() {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TSID_SINGLE_PREFIX_BYTE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DIMENSION_GROUP_SORT)
        );
        assertThat(TsidLayout.forIndex(indexMetadata(version, Settings.EMPTY)).kind(), equalTo(TsidLayout.Kind.SINGLE_BYTE_PREFIX));
        Settings explicit = Settings.builder().put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "dim.host").build();
        assertThat(TsidLayout.forIndex(indexMetadata(version, explicit)).kind(), equalTo(TsidLayout.Kind.SINGLE_BYTE_PREFIX));
    }

    public void testNoPrefixWithDimensionGroupSort() {
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TIME_SERIES_DIMENSION_GROUP_SORT);
        // enabled by default
        assertThat(TsidLayout.forIndex(indexMetadata(version, Settings.EMPTY)).kind(), equalTo(TsidLayout.Kind.NO_PREFIX));
        assertThat(TsidLayout.forVersion(version).kind(), equalTo(TsidLayout.Kind.NO_PREFIX));
        // explicit field
        Settings explicit = Settings.builder().put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "dim.host").build();
        assertThat(TsidLayout.forIndex(indexMetadata(version, explicit)).kind(), equalTo(TsidLayout.Kind.NO_PREFIX));
        // an empty field disables the grouped sort and restores the prefix byte
        Settings disabled = Settings.builder().put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "").build();
        assertThat(TsidLayout.forIndex(indexMetadata(version, disabled)).kind(), equalTo(TsidLayout.Kind.SINGLE_BYTE_PREFIX));
    }
}
