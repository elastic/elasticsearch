/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DocsStatsSerializationTests extends AbstractXContentSerializingTestCase<DocsStats> {
    @Override
    protected DocsStats doParseInstance(XContentParser parser) throws IOException {
        return DocsStats.PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<DocsStats> instanceReader() {
        return DocsStats::new;
    }

    @Override
    protected DocsStats createTestInstance() {
        return new DocsStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected DocsStats mutateInstance(DocsStats instance) {
        switch (between(0, 2)) {
            case 0:
                return new DocsStats(
                    randomValueOtherThan(instance.getCount(), ESTestCase::randomNonNegativeLong),
                    instance.getDeleted(),
                    instance.getTotalSizeInBytes()
                );
            case 1:
                return new DocsStats(
                    instance.getCount(),
                    randomValueOtherThan(instance.getDeleted(), ESTestCase::randomNonNegativeLong),
                    instance.getTotalSizeInBytes()
                );
            case 2:
                return new DocsStats(
                    instance.getCount(),
                    instance.getDeleted(),
                    randomValueOtherThan(instance.getTotalSizeInBytes(), ESTestCase::randomNonNegativeLong)
                );
            default:
                throw new AssertionError("failure, illegal switch case");
        }
    }

}
