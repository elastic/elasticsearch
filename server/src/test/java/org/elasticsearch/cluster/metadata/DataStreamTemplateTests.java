/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamAliasTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class DataStreamTemplateTests extends AbstractSerializingTestCase<DataStreamTemplate> {

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

    public static DataStreamTemplate randomInstance() {
        if (randomBoolean()) {
            return new ComposableIndexTemplate.DataStreamTemplate();
        }

        boolean hidden = randomBoolean();
        Map<String, DataStreamAliasTemplate> aliases = null;
        if (randomBoolean()) {
            aliases = new HashMap<>();
            int numAliases = randomIntBetween(1, 5);
            for (int i = 0; i < numAliases; i++) {
                ComposableIndexTemplate.DataStreamAliasTemplate instance = randomAliasInstance();
                aliases.put(instance.getAlias(), instance);
            }
        }
        return new ComposableIndexTemplate.DataStreamTemplate(hidden, aliases);
    }

    static DataStreamAliasTemplate randomAliasInstance() {
        String alias = randomAlphaOfLength(4);
        Boolean writeAlias = null;
        if (randomBoolean()) {
            writeAlias = randomBoolean();
        }
        return new DataStreamAliasTemplate(alias, writeAlias);
    }
}
