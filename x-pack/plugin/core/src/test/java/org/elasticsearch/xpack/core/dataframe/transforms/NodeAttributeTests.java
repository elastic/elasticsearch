/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Predicate;


public class NodeAttributeTests extends AbstractSerializingTestCase<NodeAttributes> {

    public static NodeAttributes randomNodeAttributes() {
        return new NodeAttributes(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? Collections.emptyMap() : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)));
    }

    @Override
    protected NodeAttributes doParseInstance(XContentParser parser) throws IOException {
        return NodeAttributes.PARSER.apply(parser, null);
    }

    @Override
    protected NodeAttributes createTestInstance() {
        return randomNodeAttributes();
    }

    @Override
    protected Reader<NodeAttributes> instanceReader() {
        return NodeAttributes::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("attributes");
    }
}
