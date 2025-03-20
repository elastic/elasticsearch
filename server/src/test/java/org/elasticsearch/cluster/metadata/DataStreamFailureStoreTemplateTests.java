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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
        return new DataStreamFailureStore.Template(instance.enabled().map(v -> v == false));
    }

    @Override
    protected DataStreamFailureStore.Template doParseInstance(XContentParser parser) throws IOException {
        return DataStreamFailureStore.Template.fromXContent(parser);
    }

    static DataStreamFailureStore.Template randomFailureStoreTemplate() {
        return new DataStreamFailureStore.Template(randomBoolean());
    }

    public void testInvalidEmptyConfiguration() {
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DataStreamFailureStore.Template(randomBoolean() ? ResettableValue.undefined() : ResettableValue.reset())
        );
        assertThat(exception.getMessage(), containsString("at least one non-null configuration value"));
    }

    public void testTemplateComposition() {
        boolean enabled = randomBoolean();
        DataStreamFailureStore.Template template = new DataStreamFailureStore.Template(enabled);
        DataStreamFailureStore.Template result = DataStreamFailureStore.builder(template).composeTemplate(template).buildTemplate();
        assertThat(result, equalTo(template));

        DataStreamFailureStore.Template negatedTemplate = new DataStreamFailureStore.Template(enabled == false);
        result = DataStreamFailureStore.builder(template).composeTemplate(negatedTemplate).buildTemplate();
        ;
        assertThat(result, equalTo(negatedTemplate));
    }
}
