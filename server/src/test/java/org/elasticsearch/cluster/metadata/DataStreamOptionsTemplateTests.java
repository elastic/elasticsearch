/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DataStreamOptionsTemplateTests extends AbstractXContentSerializingTestCase<DataStreamOptions.Template> {

    public static final DataStreamOptions.Template RESET = new DataStreamOptions.Template(ResettableValue.reset());

    @Override
    protected Writeable.Reader<DataStreamOptions.Template> instanceReader() {
        return DataStreamOptions.Template::read;
    }

    @Override
    protected DataStreamOptions.Template createTestInstance() {
        return randomDataStreamOptions();
    }

    public static DataStreamOptions.Template randomDataStreamOptions() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> DataStreamOptions.Template.EMPTY;
            case 1 -> RESET;
            case 2 -> new DataStreamOptions.Template(
                ResettableValue.create(DataStreamFailureStoreTemplateTests.randomFailureStoreTemplate())
            );
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        };
    }

    @Override
    protected DataStreamOptions.Template mutateInstance(DataStreamOptions.Template instance) {
        ResettableValue<DataStreamFailureStore.Template> failureStore = instance.failureStore();
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
        return new DataStreamOptions.Template(failureStore);
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
                DataStreamLifecycle.createFailuresLifecycleTemplate(randomBoolean(), randomTimeValue())
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
            new DataStreamFailureStore.Template(null, DataStreamLifecycle.createFailuresLifecycleTemplate(true, randomPositiveTimeValue()))
        );
        result = DataStreamOptions.builder(dataStreamOptionsWithLifecycle)
            .composeTemplate(dataStreamOptionsWithoutLifecycle)
            .buildTemplate();
        assertThat(result.failureStore().get().enabled(), equalTo(dataStreamOptionsWithoutLifecycle.failureStore().get().enabled()));
        assertThat(result.failureStore().get().lifecycle(), equalTo(dataStreamOptionsWithLifecycle.failureStore().get().lifecycle()));

        // Reset
        result = DataStreamOptions.builder(fullyConfigured).composeTemplate(RESET).buildTemplate();
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));
    }

    public void testBackwardCompatibility() throws IOException {
        DataStreamOptions.Template result = copyInstance(DataStreamOptions.Template.EMPTY, TransportVersions.SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));

        DataStreamOptions.Template withEnabled = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(randomBoolean(), DataStreamLifecycleTemplateTests.randomFailuresLifecycleTemplate())
        );
        result = copyInstance(withEnabled, TransportVersions.SETTINGS_IN_DATA_STREAMS);
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
        result = copyInstance(withoutEnabled, TransportVersions.SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));

        DataStreamOptions.Template withEnabledReset = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(ResettableValue.reset(), ResettableValue.undefined())
        );
        result = copyInstance(withEnabledReset, TransportVersions.SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(new DataStreamOptions.Template(ResettableValue.reset())));
    }
}
