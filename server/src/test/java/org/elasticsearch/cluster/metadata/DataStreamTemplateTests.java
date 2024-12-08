/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
        return DataStreamTemplate.PARSER.parse(parser, null);
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
        switch (randomIntBetween(0, 1)) {
            case 0 -> hidden = hidden == false;
            case 1 -> allowCustomRouting = allowCustomRouting == false;
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        }
        return new DataStreamTemplate(hidden, allowCustomRouting);
    }

    public static DataStreamTemplate randomInstance() {
        return new ComposableIndexTemplate.DataStreamTemplate(randomBoolean(), randomBoolean());
    }

}
