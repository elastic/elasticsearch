/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.logging;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.ObjectParser;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.io.InputStream;

import static org.hamcrest.Matchers.is;

/**
 * Test to verify ES JSON log format. Used in ES v7. Some users might decide to keep that format.
 * Provide a custom log4j configuration where layout is an old style pattern and confirm that Elasticsearch can successfully startup.
 */
public class ESJsonLogsConfigIT extends JsonLogsIntegTestCase {

    private static final String NODE_NAME = "test-node-0";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .setting("xpack.security.enabled", "false")
        .withNode(localNodeSpecBuilder -> localNodeSpecBuilder.name(NODE_NAME))
        .configFile("log4j2.properties", Resource.fromClasspath("es-v7-log4j2.properties"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected InputStream openLogsStream() {
        return cluster.getNodeLog(0, LogType.SERVER_JSON);
    }

    @Override
    protected Matcher<String> nodeNameMatcher() {
        return is(NODE_NAME);
    }

    @Override
    protected ObjectParser<JsonLogLine, Void> getParser() {
        return JsonLogLine.ES_LOG_LINE;
    }
}
