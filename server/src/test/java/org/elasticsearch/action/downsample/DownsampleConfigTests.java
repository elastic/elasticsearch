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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.action.downsample.DownsampleConfig.generateDownsampleIndexName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    public static DownsampleConfig.SamplingMethod randomSamplingMethod() {
        if (between(0, DownsampleConfig.SamplingMethod.values().length) == 0) {
            return null;
        } else {
            return randomFrom(DownsampleConfig.SamplingMethod.values());
        }
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
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new DownsampleConfig((DateHistogramInterval) null, randomSamplingMethod())
        );
        assertThat(e.getMessage(), equalTo("Parameter [fixed_interval] is required."));
    }

    public void testEmptyTimezone() {
        DownsampleConfig config = new DownsampleConfig(randomInterval(), randomSamplingMethod());
        assertEquals("UTC", config.getTimeZone());
    }

    public void testSamplingMethodFromString() {
        assertThat(DownsampleConfig.SamplingMethod.fromString(null), nullValue());
        assertThat(DownsampleConfig.SamplingMethod.fromString("aggregate"), is(DownsampleConfig.SamplingMethod.AGGREGATE));
        assertThat(DownsampleConfig.SamplingMethod.fromString("last_value"), is(DownsampleConfig.SamplingMethod.LAST_VALUE));
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> DownsampleConfig.SamplingMethod.fromString("foo")
        );
        assertThat(error.getMessage(), equalTo("Sampling method [foo] is not one of the accepted methods [aggregate, last_value]."));
    }

    public void testSamplingMethodFromIndexMetadata() {
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getSettings()).thenReturn(Settings.EMPTY);
        assertThat(DownsampleConfig.SamplingMethod.fromIndexMetadata(indexMetadata), nullValue());

        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put("index.downsample.interval", "5m").build());
        assertThat(DownsampleConfig.SamplingMethod.fromIndexMetadata(indexMetadata), is(DownsampleConfig.SamplingMethod.AGGREGATE));

        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put("index.downsample.sampling_method", "aggregate").build());
        assertThat(DownsampleConfig.SamplingMethod.fromIndexMetadata(indexMetadata), is(DownsampleConfig.SamplingMethod.AGGREGATE));

        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put("index.downsample.sampling_method", "last_value").build());
        assertThat(DownsampleConfig.SamplingMethod.fromIndexMetadata(indexMetadata), is(DownsampleConfig.SamplingMethod.LAST_VALUE));
    }

    public void testEffectiveSamplingMethod() {
        assertThat(DownsampleConfig.SamplingMethod.getOrDefault(null), is(DownsampleConfig.SamplingMethod.AGGREGATE));
        assertThat(
            DownsampleConfig.SamplingMethod.getOrDefault(DownsampleConfig.SamplingMethod.AGGREGATE),
            is(DownsampleConfig.SamplingMethod.AGGREGATE)
        );
        assertThat(
            DownsampleConfig.SamplingMethod.getOrDefault(DownsampleConfig.SamplingMethod.LAST_VALUE),
            is(DownsampleConfig.SamplingMethod.LAST_VALUE)
        );

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
