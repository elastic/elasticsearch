/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.common.logging.JsonLogsIntegTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.io.InputStream;

import static org.hamcrest.Matchers.is;

public class JsonLogsFormatAndParseIT extends JsonLogsIntegTestCase {

    private static final String NODE_NAME = "test-node-0";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .withNode(localNodeSpecBuilder -> localNodeSpecBuilder.name(NODE_NAME))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Matcher<String> nodeNameMatcher() {
        return is(NODE_NAME);
    }

    @Override
    protected InputStream openLogsStream() {
        return cluster.getNodeLog(0, LogType.SERVER_JSON);
    }
}
