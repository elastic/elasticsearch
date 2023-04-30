/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class RollupJobTests extends SimpleDiffableSerializationTestCase<RollupJob> {
    @Override
    protected Writeable.Reader<Diff<RollupJob>> diffReader() {
        return RollupJob::readJobDiffFrom;
    }

    @Override
    protected RollupJob doParseInstance(XContentParser parser) throws IOException {
        return RollupJob.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RollupJob> instanceReader() {
        return RollupJob::new;
    }

    @Override
    protected RollupJob createTestInstance() {
        if (randomBoolean()) {
            return new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), null);
        }

        Map<String, String> headers = Collections.emptyMap();
        if (randomBoolean()) {
            headers = Maps.newMapWithExpectedSize(1);
            headers.put("foo", "bar");
        }
        return new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), headers);
    }

    @Override
    protected RollupJob mutateInstance(RollupJob instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected RollupJob makeTestChanges(RollupJob other) {
        if (randomBoolean()) {
            if (other.getHeaders().isEmpty()) {
                Map<String, String> headers = Maps.newMapWithExpectedSize(1);
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
