/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class DataStreamLifecycleTemplateTests extends AbstractXContentSerializingTestCase<DataStreamLifecycle.Template> {

    @Override
    protected Writeable.Reader<DataStreamLifecycle.Template> instanceReader() {
        return DataStreamLifecycle.Template::read;
    }

    @Override
    protected DataStreamLifecycle.Template createTestInstance() {
        return randomLifecycleTemplate();
    }

    @Override
    protected DataStreamLifecycle.Template mutateInstance(DataStreamLifecycle.Template instance) throws IOException {
        var enabled = instance.enabled();
        var retention = instance.dataRetention();
        var downsampling = instance.downsampling();
        switch (randomInt(2)) {
            case 0 -> enabled = enabled == false;
            case 1 -> retention = randomValueOtherThan(retention, DataStreamLifecycleTemplateTests::randomRetention);
            case 2 -> downsampling = randomValueOtherThan(downsampling, DataStreamLifecycleTemplateTests::randomDownsampling);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DataStreamLifecycle.Template(enabled, retention, downsampling);
    }

    @Override
    protected DataStreamLifecycle.Template doParseInstance(XContentParser parser) throws IOException {
        return DataStreamLifecycle.Template.fromXContent(parser);
    }

    public void testInvalidDownsamplingConfiguration() {
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.builder()
                    .downsampling(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueDays(10),
                                new DownsampleConfig(new DateHistogramInterval("2h"))
                            ),
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueDays(3),
                                new DownsampleConfig(new DateHistogramInterval("2h"))
                            )
                        )
                    )
                    .buildTemplate()
            );
            assertThat(
                exception.getMessage(),
                equalTo("A downsampling round must have a later 'after' value than the proceeding, 3d is not after 10d.")
            );
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.builder()
                    .downsampling(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueDays(10),
                                new DownsampleConfig(new DateHistogramInterval("2h"))
                            ),
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueDays(30),
                                new DownsampleConfig(new DateHistogramInterval("2h"))
                            )
                        )
                    )
                    .buildTemplate()
            );
            assertThat(exception.getMessage(), equalTo("Downsampling interval [2h] must be greater than the source index interval [2h]."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.builder()
                    .downsampling(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueDays(10),
                                new DownsampleConfig(new DateHistogramInterval("2h"))
                            ),
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueDays(30),
                                new DownsampleConfig(new DateHistogramInterval("3h"))
                            )
                        )
                    )
                    .buildTemplate()
            );
            assertThat(exception.getMessage(), equalTo("Downsampling interval [3h] must be a multiple of the source index interval [2h]."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.builder().downsampling((List.of())).buildTemplate()
            );
            assertThat(exception.getMessage(), equalTo("Downsampling configuration should have at least one round configured."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.builder()
                    .downsampling(
                        Stream.iterate(1, i -> i * 2)
                            .limit(12)
                            .map(
                                i -> new DataStreamLifecycle.DownsamplingRound(
                                    TimeValue.timeValueDays(i),
                                    new DownsampleConfig(new DateHistogramInterval(i + "h"))
                                )
                            )
                            .toList()
                    )
                    .buildTemplate()
            );
            assertThat(exception.getMessage(), equalTo("Downsampling configuration supports maximum 10 configured rounds. Found: 12"));
        }

        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.builder()
                    .downsampling(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueDays(10),
                                new DownsampleConfig(new DateHistogramInterval("2m"))
                            )
                        )
                    )
                    .buildTemplate()
            );
            assertThat(
                exception.getMessage(),
                equalTo("A downsampling round must have a fixed interval of at least five minutes but found: 2m")
            );
        }
    }

    public static DataStreamLifecycle.Template randomLifecycleTemplate() {
        return new DataStreamLifecycle.Template(randomBoolean(), randomRetention(), randomDownsampling());
    }

    private static ResettableValue<TimeValue> randomRetention() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> ResettableValue.undefined();
            case 1 -> ResettableValue.reset();
            case 2 -> ResettableValue.create(TimeValue.timeValueDays(randomIntBetween(1, 100)));
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }

    private static ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> randomDownsampling() {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> ResettableValue.reset();
            case 1 -> ResettableValue.create(DataStreamLifecycleTests.randomDownsampling());
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }
}
