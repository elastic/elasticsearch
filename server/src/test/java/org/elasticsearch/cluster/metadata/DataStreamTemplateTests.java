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
import org.elasticsearch.index.IndexMode;
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
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static DataStreamTemplate randomInstance() {
        IndexMode indexMode = randomBoolean() ? randomFrom(IndexMode.values()) : null;
        return new ComposableIndexTemplate.DataStreamTemplate(randomBoolean(), randomBoolean());
    }

}
