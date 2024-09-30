/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.downsample;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.action.downsample.DownsampleConfig.generateDownsampleIndexName;
import static org.hamcrest.Matchers.is;

public class DonwsampleConfigTests extends ESTestCase {

    public void testGenerateDownsampleIndexName() {
        {
            String indexName = "test";
            IndexMetadata indexMeta = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
            assertThat(generateDownsampleIndexName("downsample-", indexMeta, new DateHistogramInterval("1h")), is("downsample-1h-test"));
        }

        {
            String downsampledIndex = "downsample-1h-test";
            IndexMetadata indexMeta = IndexMetadata.builder(downsampledIndex)
                .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY, "test"))
                .build();
            assertThat(generateDownsampleIndexName("downsample-", indexMeta, new DateHistogramInterval("8h")), is("downsample-8h-test"));
        }

        {
            // test origin takes higher precedence than the configured source setting
            String downsampledIndex = "downsample-1h-test";
            IndexMetadata indexMeta = IndexMetadata.builder(downsampledIndex)
                .settings(
                    indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY, "test")
                        .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY, "downsample-1s-test")
                )
                .build();
            assertThat(generateDownsampleIndexName("downsample-", indexMeta, new DateHistogramInterval("8h")), is("downsample-8h-test"));
        }
    }
}
