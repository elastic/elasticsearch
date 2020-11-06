/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.latest;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.util.Collections;

public class LatestDocConfigTests extends AbstractSerializingTransformTestCase<LatestDocConfig> {

    public static LatestDocConfig randomLatestDocConfig() {
        return new LatestDocConfig(
            randomList(1, 10, () -> randomAlphaOfLengthBetween(1, 10)),
            Collections.singletonList(SortBuilders.fieldSort(randomAlphaOfLengthBetween(1, 10))));
    }

    @Override
    protected LatestDocConfig doParseInstance(XContentParser parser) throws IOException {
        return LatestDocConfig.fromXContent(parser, false);
    }

    @Override
    protected LatestDocConfig createTestInstance() {
        return randomLatestDocConfig();
    }

    @Override
    protected Reader<LatestDocConfig> instanceReader() {
        return LatestDocConfig::new;
    }
}
