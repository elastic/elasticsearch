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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.action.downsample.DownsampleConfig.generateDownsampleIndexName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DownsampleConfigTests extends AbstractXContentSerializingTestCase<DownsampleConfig> {

    @Override
    protected DownsampleConfig createTestInstance() {
        return randomConfig();
    }

    @Override
    protected DownsampleConfig mutateInstance(DownsampleConfig instance) {
        var interval = instance.getFixedInterval();
        var samplingMode = instance.getSamplingMethod();
        switch (between(0, 1)) {
            case 0 -> interval = randomValueOtherThan(interval, DownsampleConfigTests::randomInterval);
            case 1 -> samplingMode = randomValueOtherThan(samplingMode, DownsampleConfigTests::randomSamplingMethod);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DownsampleConfig(interval, samplingMode);
    }

    public static DownsampleConfig randomConfig() {
        return new DownsampleConfig(randomInterval(), randomSamplingMethod());
    }

    private static DownsampleConfig.SamplingMethod randomSamplingMethod() {
        return randomBoolean() ? null : randomFrom(DownsampleConfig.SamplingMethod.values());
    }

    @Override
    protected Writeable.Reader<DownsampleConfig> instanceReader() {
        return DownsampleConfig::new;
    }

    @Override
    protected DownsampleConfig doParseInstance(final XContentParser parser) throws IOException {
        return DownsampleConfig.fromXContent(parser);
    }

    public void testEmptyFixedInterval() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new DownsampleConfig((DateHistogramInterval) null));
        assertThat(e.getMessage(), equalTo("Parameter [fixed_interval] is required."));
    }

    public void testEmptyTimezone() {
        DownsampleConfig config = new DownsampleConfig(randomInterval());
        assertEquals("UTC", config.getTimeZone());
    }

    public void testValidateSourceAndTargetConfiguration() {
        var sourceInterval = new DateHistogramInterval("1h");
        var targetInterval = new DateHistogramInterval("1d");
        var lastValue = DownsampleConfig.SamplingMethod.LAST_VALUE;
        var aggregate = randomBoolean() ? null : DownsampleConfig.SamplingMethod.AGGREGATE;

        DownsampleConfig.validateSourceAndTargetConfiguration(
            new DownsampleConfig(sourceInterval, aggregate),
            new DownsampleConfig(targetInterval, aggregate)
        );

        DownsampleConfig.validateSourceAndTargetConfiguration(
            new DownsampleConfig(sourceInterval, lastValue),
            new DownsampleConfig(targetInterval, lastValue)
        );

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> DownsampleConfig.validateSourceAndTargetConfiguration(
                new DownsampleConfig(sourceInterval, lastValue),
                new DownsampleConfig(targetInterval, aggregate)
            )
        );
        assertThat(
            error.getMessage(),
            equalTo("Downsampling method [aggregate] is not compatible with the source index downsampling method [last_value].")
        );

        error = expectThrows(
            IllegalArgumentException.class,
            () -> DownsampleConfig.validateSourceAndTargetConfiguration(
                new DownsampleConfig(sourceInterval, aggregate),
                new DownsampleConfig(targetInterval, lastValue)
            )
        );
        assertThat(
            error.getMessage(),
            equalTo("Downsampling method [last_value] is not compatible with the source index downsampling method [aggregate].")
        );

        error = expectThrows(
            IllegalArgumentException.class,
            () -> DownsampleConfig.validateSourceAndTargetConfiguration(
                new DownsampleConfig(targetInterval, aggregate),
                new DownsampleConfig(sourceInterval, aggregate)
            )
        );
        assertThat(error.getMessage(), equalTo("Downsampling interval [1h] must be greater than the source index interval [1d]."));
    }

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

    public static DateHistogramInterval randomInterval() {
        return new DateHistogramInterval(randomIntBetween(1, 1000) + randomFrom("m", "h", "d"));
    }
}
