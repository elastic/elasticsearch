/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.dataframe.transforms.pivot.SingleGroupSource.Type.TERMS;

public class GroupConfigTests extends AbstractSerializingTestCase<GroupConfig> {

    public static GroupConfig randomGroupConfig() {
        String targetFieldName = randomAlphaOfLengthBetween(1, 20);
        return new GroupConfig(targetFieldName, TERMS, TermsGroupSourceTests.randomTermsGroupSource());
    }

    @Override
    protected GroupConfig doParseInstance(XContentParser parser) throws IOException {
        return GroupConfig.fromXContent(parser, false);
    }

    @Override
    protected GroupConfig createTestInstance() {
        return randomGroupConfig();
    }

    @Override
    protected Reader<GroupConfig> instanceReader() {
        return GroupConfig::new;
    }
}
