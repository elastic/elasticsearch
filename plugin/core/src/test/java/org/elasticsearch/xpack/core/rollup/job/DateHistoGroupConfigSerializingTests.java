/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.io.IOException;

public class DateHistoGroupConfigSerializingTests extends AbstractSerializingTestCase<DateHistoGroupConfig> {
    @Override
    protected DateHistoGroupConfig doParseInstance(XContentParser parser) throws IOException {
        return DateHistoGroupConfig.PARSER.apply(parser, null).build();
    }

    @Override
    protected Writeable.Reader<DateHistoGroupConfig> instanceReader() {
        return DateHistoGroupConfig::new;
    }

    @Override
    protected DateHistoGroupConfig createTestInstance() {
        return ConfigTestHelpers.getDateHisto().build();
    }
}
