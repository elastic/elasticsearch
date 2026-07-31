/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DataStreamOptionsTemplateTests extends AbstractXContentSerializingTestCase<DataStreamOptions.Template> {

    public static final DataStreamOptions.Template RESET = new DataStreamOptions.Template(ResettableValue.reset());
    private static final TransportVersion SETTINGS_IN_DATA_STREAMS = TransportVersion.fromName("settings_in_data_streams");

    @Override
    protected Writeable.Reader<DataStreamOptions.Template> instanceReader() {
        return DataStreamOptions.Template::read;
    }

    @Override
    protected DataStreamOptions.Template createTestInstance() {
        return randomDataStreamOptions();
    }

    public static DataStreamOptions.Template randomDataStreamOptions() {
        return switch (randomIntBetween(0, 4)) {
            case 0 -> DataStreamOptions.Template.EMPTY;
            case 1 -> RESET;
            case 2 -> new DataStreamOptions.Template(
                ResettableValue.create(DataStreamFailureStoreTemplateTests.randomFailureStoreTemplate())
            );
            case 3 -> new DataStreamOptions.Template(DataStreamDerivedMetricsTests.randomTemplate());
            case 4 -> new DataStreamOptions.Template(
                ResettableValue.create(DataStreamFailureStoreTemplateTests.randomFailureStoreTemplate()),
                ResettableValue.create(DataStreamDerivedMetricsTests.randomTemplate())
            );
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        };
    }

    @Override
    protected DataStreamOptions.Template mutateInstance(DataStreamOptions.Template instance) {
        ResettableValue<DataStreamFailureStore.Template> failureStore = instance.failureStore();
        ResettableValue<DataStreamDerivedMetrics.Template> derivedMetrics = instance.derivedMetrics();
        if (randomBoolean()) {
            if (failureStore.isDefined() == false) {
                failureStore = randomBoolean()
                    ? ResettableValue.create(DataStreamFailureStoreTemplateTests.randomFailureStoreTemplate())
                    : ResettableValue.reset();
            } else if (failureStore.shouldReset()) {
                failureStore = ResettableValue.create(
                    randomBoolean() ? DataStreamFailureStoreTemplateTests.randomFailureStoreTemplate() : null
                );
            } else {
                failureStore = switch (randomIntBetween(0, 2)) {
                    case 0 -> ResettableValue.undefined();
                    case 1 -> ResettableValue.reset();
                    case 2 -> ResettableValue.create(
                        randomValueOtherThan(failureStore.get(), DataStreamFailureStoreTemplateTests::randomFailureStoreTemplate)
                    );
                    default -> throw new IllegalArgumentException("Illegal randomisation branch");
                };
            }
        } else {
            if (derivedMetrics.isDefined() == false) {
                derivedMetrics = randomBoolean()
                    ? ResettableValue.create(DataStreamDerivedMetricsTests.randomTemplate())
                    : ResettableValue.reset();
            } else if (derivedMetrics.shouldReset()) {
                derivedMetrics = ResettableValue.create(randomBoolean() ? DataStreamDerivedMetricsTests.randomTemplate() : null);
            } else {
                derivedMetrics = switch (randomIntBetween(0, 2)) {
                    case 0 -> ResettableValue.undefined();
                    case 1 -> ResettableValue.reset();
                    case 2 -> ResettableValue.create(
                        randomValueOtherThan(derivedMetrics.get(), DataStreamDerivedMetricsTests::randomTemplate)
                    );
                    default -> throw new IllegalArgumentException("Illegal randomisation branch");
                };
            }
        }
        return new DataStreamOptions.Template(failureStore, derivedMetrics);
    }

    @Override
    protected DataStreamOptions.Template doParseInstance(XContentParser parser) throws IOException {
        return DataStreamOptions.Template.fromXContent(parser);
    }

    public void testTemplateComposition() {
        // we fully define the options to avoid having to check for normalised values in the assertion
        DataStreamOptions.Template fullyConfigured = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(
                randomBoolean(),
                DataStreamLifecycle.failuresLifecycleBuilder().enabled(randomBoolean()).dataRetention(randomTimeValue()).buildTemplate()
            )
        );

        // No updates
        DataStreamOptions.Template result = DataStreamOptions.builder(DataStreamOptions.Template.EMPTY).buildTemplate();
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));
        result = DataStreamOptions.builder(fullyConfigured).buildTemplate();
        assertThat(result, equalTo(fullyConfigured));

        // Explicit nulls are normalised
        result = DataStreamOptions.builder(RESET).buildTemplate();
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));

        // Merge
        result = DataStreamOptions.builder(fullyConfigured).composeTemplate(DataStreamOptions.Template.EMPTY).buildTemplate();
        assertThat(result, equalTo(fullyConfigured));

        // Override
        DataStreamOptions.Template negated = new DataStreamOptions.Template(
            fullyConfigured.failureStore()
                .map(
                    failureStore -> DataStreamFailureStore.builder(failureStore)
                        .enabled(failureStore.enabled().map(enabled -> enabled == false))
                        .buildTemplate()
                )
        );
        result = DataStreamOptions.builder(fullyConfigured).composeTemplate(negated).buildTemplate();
        assertThat(result, equalTo(negated));

        // Test merging
        DataStreamOptions.Template dataStreamOptionsWithoutLifecycle = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(true, null)
        );
        DataStreamOptions.Template dataStreamOptionsWithLifecycle = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(
                null,
                DataStreamLifecycle.failuresLifecycleBuilder().enabled(true).dataRetention(randomPositiveTimeValue()).buildTemplate()
            )
        );
        result = DataStreamOptions.builder(dataStreamOptionsWithLifecycle)
            .composeTemplate(dataStreamOptionsWithoutLifecycle)
            .buildTemplate();
        assertThat(result.failureStore().get().enabled(), equalTo(dataStreamOptionsWithoutLifecycle.failureStore().get().enabled()));
        assertThat(result.failureStore().get().lifecycle(), equalTo(dataStreamOptionsWithLifecycle.failureStore().get().lifecycle()));

        DataStreamDerivedMetrics.Metric requests = new DataStreamDerivedMetrics.Metric(
            "http.requests",
            DataStreamDerivedMetrics.MetricType.COUNTER,
            null,
            null,
            null,
            List.of("http.request.method"),
            null
        );
        DataStreamDerivedMetrics.Destination tenSeconds = new DataStreamDerivedMetrics.Destination(TimeValue.timeValueSeconds(10), null);
        DataStreamDerivedMetrics.Destination oneMinute = new DataStreamDerivedMetrics.Destination(TimeValue.timeValueMinutes(1), null);
        DataStreamOptions.Template derivedBase = new DataStreamOptions.Template(
            new DataStreamDerivedMetrics.Template(
                true,
                List.of("ingest.docs.rate"),
                TimeValue.timeValueSeconds(10),
                List.of(tenSeconds),
                List.of("service.name"),
                List.of(requests)
            )
        );
        DataStreamOptions.Template derivedExtra = new DataStreamOptions.Template(
            new DataStreamDerivedMetrics.Template(
                null,
                List.of("ingest.failures.rate"),
                null,
                List.of(oneMinute),
                List.of("host.name"),
                List.of()
            )
        );
        result = DataStreamOptions.builder(derivedBase).composeTemplate(derivedExtra).buildTemplate();
        assertThat(result.derivedMetrics().get().builtin(), equalTo(List.of("ingest.docs.rate", "ingest.failures.rate")));
        // A more specific template does not redefine the default interval, so the base one stands.
        assertThat(result.derivedMetrics().get().defaultInterval(), equalTo(TimeValue.timeValueSeconds(10)));
        assertThat(result.derivedMetrics().get().destinations(), equalTo(List.of(tenSeconds, oneMinute)));
        assertThat(result.derivedMetrics().get().dimensions(), equalTo(List.of("service.name", "host.name")));
        assertThat(result.derivedMetrics().get().metrics(), equalTo(List.of(requests)));

        // Reset
        result = DataStreamOptions.builder(fullyConfigured).composeTemplate(RESET).buildTemplate();
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));
    }

    public void testBackwardCompatibility() throws IOException {
        DataStreamOptions.Template result = copyInstance(DataStreamOptions.Template.EMPTY, SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));

        DataStreamOptions.Template withEnabled = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(randomBoolean(), DataStreamLifecycleTemplateTests.randomFailuresLifecycleTemplate())
        );
        result = copyInstance(withEnabled, SETTINGS_IN_DATA_STREAMS);
        assertThat(result.failureStore().get().enabled(), equalTo(withEnabled.failureStore().get().enabled()));
        assertThat(result.failureStore().get().lifecycle(), equalTo(ResettableValue.undefined()));

        DataStreamOptions.Template withoutEnabled = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(
                ResettableValue.undefined(),
                randomBoolean()
                    ? ResettableValue.reset()
                    : ResettableValue.create(DataStreamLifecycleTemplateTests.randomFailuresLifecycleTemplate())
            )
        );
        result = copyInstance(withoutEnabled, SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));

        DataStreamOptions.Template withEnabledReset = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(ResettableValue.reset(), ResettableValue.undefined())
        );
        result = copyInstance(withEnabledReset, SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(new DataStreamOptions.Template(ResettableValue.reset())));

        DataStreamOptions.Template withDerivedMetrics = new DataStreamOptions.Template(DataStreamDerivedMetricsTests.randomTemplate());
        result = copyInstance(withDerivedMetrics, SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));
    }
}
