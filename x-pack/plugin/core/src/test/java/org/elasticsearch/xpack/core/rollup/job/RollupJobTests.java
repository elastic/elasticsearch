/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RollupJobTests extends AbstractDiffableSerializationTestCase {
    @Override
    protected Writeable.Reader<Diff> diffReader() {
        return RollupJob::readJobDiffFrom;
    }

    @Override
    protected ToXContent doParseInstance(XContentParser parser) throws IOException {
        return RollupJob.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader instanceReader() {
        return RollupJob::new;
    }

    @Override
    protected Writeable createTestInstance() {
        if (randomBoolean()) {
            return new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), null);
        }

        Map<String, String> headers = Collections.emptyMap();
        if (randomBoolean()) {
            headers = new HashMap<>(1);
            headers.put("foo", "bar");
        }
        return new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), headers);
    }

    @Override
    protected Diffable makeTestChanges(Diffable testInstance) {
        RollupJob other = (RollupJob) testInstance;
        if (randomBoolean()) {
            if (other.getHeaders().isEmpty()) {
                Map<String, String> headers = new HashMap<>(1);
                headers.put("foo", "bar");
                return new RollupJob(other.getConfig(), headers);
            } else {
                return new RollupJob(other.getConfig(), null);
            }
        } else {
            return new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), other.getHeaders());
        }
    }
}
