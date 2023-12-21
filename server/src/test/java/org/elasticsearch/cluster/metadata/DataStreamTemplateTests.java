/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataStreamTemplateTests extends AbstractXContentSerializingTestCase<DataStreamTemplate> {

    @Override
    protected DataStreamTemplate doParseInstance(XContentParser parser) throws IOException {
        return DataStreamTemplate.INTERNAL_PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<DataStreamTemplate> instanceReader() {
        return DataStreamTemplate::new;
    }

    @Override
    protected DataStreamTemplate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected DataStreamTemplate mutateInstance(DataStreamTemplate instance) {
        var hidden = instance.isHidden();
        var allowCustomRouting = instance.isAllowCustomRouting();
        var failureStore = instance.hasFailureStore();
        switch (randomIntBetween(0, 2)) {
            case 0 -> hidden = hidden == false;
            case 1 -> allowCustomRouting = allowCustomRouting == false;
            default -> failureStore = failureStore == false;
        }
        return new DataStreamTemplate(hidden, allowCustomRouting, failureStore);
    }

    public static DataStreamTemplate randomInstance() {
        return new ComposableIndexTemplate.DataStreamTemplate(randomBoolean(), randomBoolean(), randomBoolean());
    }
}
