/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamFailureStoreTemplateTests extends AbstractXContentSerializingTestCase<DataStreamFailureStore.Template> {

    @Override
    protected Writeable.Reader<DataStreamFailureStore.Template> instanceReader() {
        return DataStreamFailureStore.Template::read;
    }

    @Override
    protected DataStreamFailureStore.Template createTestInstance() {
        return randomFailureStoreTemplate();
    }

    @Override
    protected DataStreamFailureStore.Template mutateInstance(DataStreamFailureStore.Template instance) {
        var enabled = instance.enabled();
        var lifecycle = instance.lifecycle();
        switch (randomIntBetween(0, 1)) {
            case 0 -> enabled = enabled.get() != null && lifecycle.get() != null && randomBoolean()
                ? randomEmptyResettableValue()
                : ResettableValue.create(Boolean.FALSE.equals(enabled.get()));
            case 1 -> lifecycle = lifecycle.get() != null && enabled.get() != null && randomBoolean()
                ? randomEmptyResettableValue()
                : ResettableValue.create(
                    randomValueOtherThan(lifecycle.get(), DataStreamLifecycleTemplateTests::randomFailuresLifecycleTemplate)
                );
            default -> throw new IllegalArgumentException("illegal randomisation branch");
        }
        return new DataStreamFailureStore.Template(enabled, lifecycle);
    }

    @Override
    protected DataStreamFailureStore.Template doParseInstance(XContentParser parser) throws IOException {
        return DataStreamFailureStore.Template.fromXContent(parser);
    }

    static DataStreamFailureStore.Template randomFailureStoreTemplate() {
        boolean enabledDefined = randomBoolean();
        boolean lifecycleDefined = enabledDefined == false || randomBoolean();
        return new DataStreamFailureStore.Template(
            enabledDefined ? ResettableValue.create(randomBoolean()) : randomEmptyResettableValue(),
            lifecycleDefined
                ? ResettableValue.create(DataStreamLifecycleTemplateTests.randomFailuresLifecycleTemplate())
                : randomEmptyResettableValue()
        );
    }

    public void testInvalidEmptyConfiguration() {
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DataStreamFailureStore.Template(ResettableValue.undefined(), ResettableValue.undefined())
        );
        assertThat(exception.getMessage(), containsString("at least one non-null configuration value"));
    }

    public void testTemplateComposition() {
        // Merging a template with itself, remains the same
        boolean enabled = randomBoolean();
        DataStreamFailureStore.Template template = new DataStreamFailureStore.Template(
            enabled,
            randomBoolean() ? null : DataStreamLifecycleTemplateTests.randomFailuresLifecycleTemplate()
        );
        DataStreamFailureStore.Template result = DataStreamFailureStore.builder(template).composeTemplate(template).buildTemplate();
        assertThat(result, equalTo(normalise(template)));

        // Override only enabled and keep lifecycle undefined
        DataStreamFailureStore.Template negatedEnabledTemplate = DataStreamFailureStore.builder(template)
            .enabled(enabled == false)
            .buildTemplate();
        result = DataStreamFailureStore.builder(template).composeTemplate(negatedEnabledTemplate).buildTemplate();
        assertThat(result, equalTo(normalise(new DataStreamFailureStore.Template(enabled == false, template.lifecycle().get()))));

        // Override only lifecycle and ensure it is merged
        enabled = false; // Ensure it's not the default to ensure that it will not be overwritten
        TimeValue retention = randomPositiveTimeValue();
        DataStreamFailureStore.Template template1 = DataStreamFailureStore.builder()
            .lifecycle(DataStreamLifecycle.failuresLifecycleBuilder().dataRetention(retention).build())
            .buildTemplate();
        DataStreamFailureStore.Template template2 = DataStreamFailureStore.builder()
            .lifecycle(DataStreamLifecycle.failuresLifecycleBuilder().enabled(enabled).build())
            .buildTemplate();
        result = DataStreamFailureStore.builder(template1).composeTemplate(template2).buildTemplate();
        assertThat(result.lifecycle().get().enabled(), equalTo(enabled));
        assertThat(result.lifecycle().get().dataRetention().get(), equalTo(retention));

        // Test reset
        DataStreamFailureStore.Template fullyFilledTemplate = DataStreamFailureStore.builder()
            .enabled(ResettableValue.create(randomBoolean()))
            .lifecycle(DataStreamLifecycleTests.randomFailuresLifecycle())
            .buildTemplate();
        result = DataStreamFailureStore.builder(fullyFilledTemplate)
            .composeTemplate(
                new DataStreamFailureStore.Template(
                    ResettableValue.reset(),
                    ResettableValue.create(DataStreamLifecycleTemplateTests.randomFailuresLifecycleTemplate())
                )
            )
            .buildTemplate();
        assertThat(result.enabled(), equalTo(ResettableValue.undefined()));
        assertThat(result.lifecycle(), not(equalTo(ResettableValue.undefined())));
        result = DataStreamFailureStore.builder(fullyFilledTemplate)
            .composeTemplate(new DataStreamFailureStore.Template(ResettableValue.create(randomBoolean()), ResettableValue.reset()))
            .buildTemplate();
        assertThat(result.enabled(), not(equalTo(ResettableValue.undefined())));
        assertThat(result.lifecycle(), equalTo(ResettableValue.undefined()));

        // Test resetting all values
        result = DataStreamFailureStore.builder(fullyFilledTemplate)
            .composeTemplate(new DataStreamFailureStore.Template(ResettableValue.reset(), ResettableValue.reset()))
            .buildTemplate();
        assertThat(result, nullValue());
    }

    private static <T> ResettableValue<T> randomEmptyResettableValue() {
        return randomBoolean() ? ResettableValue.undefined() : ResettableValue.reset();
    }

    private static DataStreamFailureStore.Template normalise(DataStreamFailureStore.Template failureStoreTemplate) {
        return new DataStreamFailureStore.Template(
            failureStoreTemplate.enabled(),
            failureStoreTemplate.lifecycle()
                .map(
                    template -> new DataStreamLifecycle.Template(
                        template.lifecycleType(),
                        template.enabled(),
                        template.dataRetention().get(),
                        template.downsampling().get()
                    )
                )
        );
    }
}
