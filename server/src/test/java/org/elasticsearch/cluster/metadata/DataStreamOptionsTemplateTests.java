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
        return switch (randomIntBetween(0, 3)) {
            case 0 -> DataStreamOptions.Template.EMPTY;
            case 1 -> createTemplateWithFailureStoreConfig(true);
            case 2 -> createTemplateWithFailureStoreConfig(false);
            case 3 -> RESET;
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        };
    }

    @Override
    protected DataStreamOptions.Template mutateInstance(DataStreamOptions.Template instance) {
        ResettableValue<DataStreamFailureStore.Template> failureStore = instance.failureStore();
        if (failureStore.isDefined() == false) {
            if (randomBoolean()) {
                return createTemplateWithFailureStoreConfig(randomBoolean());
            } else {
                return new DataStreamOptions.Template(ResettableValue.reset());
            }
        }
        if (failureStore.shouldReset()) {
            if (randomBoolean()) {
                return createTemplateWithFailureStoreConfig(randomBoolean());
            } else {
                return DataStreamOptions.Template.EMPTY;
            }
        }
        return new DataStreamOptions.Template(
            instance.failureStore().map(x -> new DataStreamFailureStore.Template(x.enabled().map(e -> e == false)))
        );
    }

    @Override
    protected DataStreamOptions.Template doParseInstance(XContentParser parser) throws IOException {
        return DataStreamOptions.Template.fromXContent(parser);
    }

    private static DataStreamOptions.Template createTemplateWithFailureStoreConfig(boolean enabled) {
        return new DataStreamOptions.Template(ResettableValue.create(new DataStreamFailureStore.Template(ResettableValue.create(enabled))));
    }

    public void testTemplateComposition() {
        DataStreamOptions.Template fullyConfigured = new DataStreamOptions.Template(new DataStreamFailureStore.Template(randomBoolean()));
        DataStreamOptions.Template negated = new DataStreamOptions.Template(
            new DataStreamFailureStore.Template(fullyConfigured.failureStore().get().enabled().get() == false)
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
        result = DataStreamOptions.builder(fullyConfigured).composeTemplate(negated).buildTemplate();
        assertThat(result, equalTo(negated));

        // Reset
        result = DataStreamOptions.builder(fullyConfigured).composeTemplate(RESET).buildTemplate();
        assertThat(result, equalTo(DataStreamOptions.Template.EMPTY));
    }
}
