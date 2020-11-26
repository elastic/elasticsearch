/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;

import java.io.IOException;
import java.util.Collections;

public class JobWrapperSerializingTests extends AbstractSerializingTestCase<GetRollupJobsAction.JobWrapper> {
    @Override
    protected GetRollupJobsAction.JobWrapper doParseInstance(XContentParser parser) throws IOException {
        return GetRollupJobsAction.JobWrapper.PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<GetRollupJobsAction.JobWrapper> instanceReader() {
        return GetRollupJobsAction.JobWrapper::new;
    }

    @Override
    protected GetRollupJobsAction.JobWrapper createTestInstance() {
        IndexerState state = null;
        int num = randomIntBetween(0,3);
        if (num == 0) {
            state = IndexerState.STOPPED;
        } else if (num == 1) {
            state = IndexerState.STARTED;
        } else if (num == 2) {
            state = IndexerState.STOPPING;
        } else if (num == 3) {
            state = IndexerState.ABORTING;
        }

        return new GetRollupJobsAction.JobWrapper(ConfigTestHelpers.randomRollupJobConfig(random()),
                new RollupIndexerJobStats(randomNonNegativeLong(), randomNonNegativeLong(),
                    randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                    randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                    randomNonNegativeLong(), randomNonNegativeLong()),
                new RollupJobStatus(state, Collections.emptyMap()));
    }
}
