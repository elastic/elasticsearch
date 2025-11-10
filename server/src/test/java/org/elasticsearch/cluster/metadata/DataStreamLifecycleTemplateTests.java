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
import org.elasticsearch.action.downsample.DownsampleConfigTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamLifecycleTemplateTests extends AbstractWireSerializingTestCase<DataStreamLifecycle.Template> {

    @Override
    protected Writeable.Reader<DataStreamLifecycle.Template> instanceReader() {
        return DataStreamLifecycle.Template::read;
    }

    @Override
    protected DataStreamLifecycle.Template createTestInstance() {
        return randomBoolean() ? randomDataLifecycleTemplate() : randomFailuresLifecycleTemplate();
    }

    @Override
    protected DataStreamLifecycle.Template mutateInstance(DataStreamLifecycle.Template instance) throws IOException {
        var lifecycleTarget = instance.lifecycleType();
        var enabled = instance.enabled();
        var retention = instance.dataRetention();
        var downsamplingRounds = instance.downsamplingRounds();
        var downsamplingMethod = instance.downsamplingMethod();
        switch (randomInt(4)) {
            case 0 -> {
                lifecycleTarget = lifecycleTarget == DataStreamLifecycle.LifecycleType.DATA
                    ? DataStreamLifecycle.LifecycleType.FAILURES
                    : DataStreamLifecycle.LifecycleType.DATA;
                if (lifecycleTarget == DataStreamLifecycle.LifecycleType.FAILURES) {
                    downsamplingRounds = ResettableValue.undefined();
                    downsamplingMethod = ResettableValue.undefined();
                }
            }
            case 1 -> enabled = enabled == false;
            case 2 -> retention = randomValueOtherThan(retention, DataStreamLifecycleTemplateTests::randomRetention);
            case 3 -> {
                downsamplingRounds = randomValueOtherThan(downsamplingRounds, DataStreamLifecycleTemplateTests::randomDownsamplingRounds);
                if (downsamplingRounds.get() != null) {
                    lifecycleTarget = DataStreamLifecycle.LifecycleType.DATA;
                } else {
                    downsamplingMethod = ResettableValue.undefined();
                }
            }
            case 4 -> {
                downsamplingMethod = randomValueOtherThan(downsamplingMethod, DataStreamLifecycleTemplateTests::randomDownsamplingMethod);
                if (downsamplingMethod.get() != null && downsamplingRounds.get() == null) {
                    downsamplingRounds = ResettableValue.create(DataStreamLifecycleTests.randomDownsamplingRounds());
                    lifecycleTarget = DataStreamLifecycle.LifecycleType.DATA;
                }
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DataStreamLifecycle.Template(lifecycleTarget, enabled, retention, downsamplingRounds, downsamplingMethod);
    }

    public void testDataLifecycleXContentSerialization() throws IOException {
        DataStreamLifecycle.Template lifecycle = randomDataLifecycleTemplate();
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            lifecycle.toXContent(builder, ToXContent.EMPTY_PARAMS, null, null, randomBoolean());
            String lifecycleJson = Strings.toString(builder);
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), lifecycleJson)) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                var parsed = DataStreamLifecycle.Template.dataLifecycleTemplatefromXContent(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
                assertThat(parsed, equalTo(lifecycle));
            }
        }
    }

    public void testFailuresLifecycleXContentSerialization() throws IOException {
        DataStreamLifecycle.Template lifecycle = randomFailuresLifecycleTemplate();
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            lifecycle.toXContent(builder, ToXContent.EMPTY_PARAMS, null, null, randomBoolean());
            String lifecycleJson = Strings.toString(builder);
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), lifecycleJson)) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                var parsed = DataStreamLifecycle.Template.failuresLifecycleTemplatefromXContent(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
                assertThat(parsed, equalTo(lifecycle));
            }
        }
    }

    public void testInvalidDownsamplingConfiguration() {
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.dataLifecycleBuilder()
                    .downsamplingRounds(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(10), new DateHistogramInterval("2h")),
                            new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(3), new DateHistogramInterval("2h"))
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
                () -> DataStreamLifecycle.dataLifecycleBuilder()
                    .downsamplingRounds(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(10), new DateHistogramInterval("2h")),
                            new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(30), new DateHistogramInterval("2h"))
                        )
                    )
                    .buildTemplate()
            );
            assertThat(exception.getMessage(), equalTo("Downsampling interval [2h] must be greater than the source index interval [2h]."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.dataLifecycleBuilder()
                    .downsamplingRounds(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(10), new DateHistogramInterval("2h")),
                            new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(30), new DateHistogramInterval("3h"))
                        )
                    )
                    .buildTemplate()
            );
            assertThat(exception.getMessage(), equalTo("Downsampling interval [3h] must be a multiple of the source index interval [2h]."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.dataLifecycleBuilder().downsamplingRounds((List.of())).buildTemplate()
            );
            assertThat(exception.getMessage(), equalTo("Downsampling configuration should have at least one round configured."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.dataLifecycleBuilder()
                    .downsamplingRounds(
                        Stream.iterate(1, i -> i * 2)
                            .limit(12)
                            .map(
                                i -> new DataStreamLifecycle.DownsamplingRound(
                                    TimeValue.timeValueDays(i),
                                    new DateHistogramInterval(i + "h")
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
                () -> DataStreamLifecycle.dataLifecycleBuilder()
                    .downsamplingRounds(
                        List.of(new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(10), new DateHistogramInterval("2m")))
                    )
                    .buildTemplate()
            );
            assertThat(
                exception.getMessage(),
                equalTo("A downsampling round must have a fixed interval of at least five minutes but found: 2m")
            );
        }

        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamLifecycle.dataLifecycleBuilder()
                    .downsamplingMethod(randomFrom(DownsampleConfig.SamplingMethod.values()))
                    .buildTemplate()
            );
            assertThat(
                exception.getMessage(),
                equalTo("Downsampling method can only be set when there is at least one downsampling round.")
            );
        }
    }

    public static DataStreamLifecycle.Template randomDataLifecycleTemplate() {
        ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> downsamplingRounds = randomDownsamplingRounds();
        return DataStreamLifecycle.createDataLifecycleTemplate(
            randomBoolean(),
            randomRetention(),
            downsamplingRounds,
            downsamplingRounds.get() == null
                ? randomBoolean() ? ResettableValue.undefined() : ResettableValue.reset()
                : randomDownsamplingMethod()
        );
    }

    public void testInvalidLifecycleConfiguration() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DataStreamLifecycle.Template(
                DataStreamLifecycle.LifecycleType.FAILURES,
                randomBoolean(),
                randomBoolean() ? null : DataStreamLifecycleTests.randomPositiveTimeValue(),
                DataStreamLifecycleTests.randomDownsamplingRounds(),
                null
            )
        );
        assertThat(
            exception.getMessage(),
            containsString("Failure store lifecycle does not support downsampling, please remove the downsampling configuration.")
        );

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DataStreamLifecycle.Template(
                DataStreamLifecycle.LifecycleType.DATA,
                randomBoolean(),
                randomBoolean() ? null : DataStreamLifecycleTests.randomPositiveTimeValue(),
                null,
                randomFrom(DownsampleConfig.SamplingMethod.values())
            )
        );
        assertThat(
            exception.getMessage(),
            containsString("Downsampling method can only be set when there is at least one downsampling round.")
        );
    }

    /**
     * Failure store lifecycle doesn't support downsampling, this random lifecycle generator never defines
     * downsampling.
     */
    public static DataStreamLifecycle.Template randomFailuresLifecycleTemplate() {
        return new DataStreamLifecycle.Template(
            DataStreamLifecycle.LifecycleType.FAILURES,
            randomBoolean(),
            randomRetention(),
            ResettableValue.undefined(),
            ResettableValue.undefined()
        );
    }

    private static ResettableValue<TimeValue> randomRetention() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> ResettableValue.undefined();
            case 1 -> ResettableValue.reset();
            case 2 -> ResettableValue.create(TimeValue.timeValueDays(randomIntBetween(1, 100)));
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }

    private static ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> randomDownsamplingRounds() {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> ResettableValue.reset();
            case 1 -> ResettableValue.create(DataStreamLifecycleTests.randomDownsamplingRounds());
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }

    private static ResettableValue<DownsampleConfig.SamplingMethod> randomDownsamplingMethod() {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> ResettableValue.reset();
            case 1 -> ResettableValue.create(DownsampleConfigTests.randomSamplingMethod());
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }
}
