/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

public class PartitionScoreTests extends AbstractSerializingTestCase<PartitionScore> {

    @Override
    protected PartitionScore createTestInstance() {
        return new PartitionScore(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20), randomDouble(), randomDouble(),
                randomDouble());
    }

    @Override
    protected Reader<PartitionScore> instanceReader() {
        return PartitionScore::new;
    }

    @Override
    protected PartitionScore parseInstance(XContentParser parser) {
        return PartitionScore.PARSER.apply(parser, null);
    }

}
