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

public class DataStreamOptionsTemplateTests extends AbstractXContentSerializingTestCase<DataStreamOptions.Template> {

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
            case 3 -> new DataStreamOptions.Template(Template.ExplicitlyNullable.empty());
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        };
    }

    @Override
    protected DataStreamOptions.Template mutateInstance(DataStreamOptions.Template instance) throws IOException {
        Template.ExplicitlyNullable<DataStreamFailureStore.Template> failureStore = instance.failureStore();
        if (failureStore == null) {
            if (randomBoolean()) {
                return createTemplateWithFailureStoreConfig(randomBoolean());
            } else {
                return new DataStreamOptions.Template(Template.ExplicitlyNullable.empty());
            }
        }
        if (failureStore.isNull()) {
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
        return new DataStreamOptions.Template(
            Template.ExplicitlyNullable.create(new DataStreamFailureStore.Template(Template.ExplicitlyNullable.create(enabled)))
        );
    }
}
