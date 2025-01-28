/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class DynamicContextDataProviderIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .plugin("logging-spi-test")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testSpiFieldIsAvailableInLogfile() throws Exception {
        try (InputStream log = cluster.getNodeLog(0, LogType.SERVER_JSON)) {
            final List<String> logLines = Streams.readAllLines(log);
            final String lastLine = logLines.get(logLines.size() - 1);
            final Map<String, Object> lineData = XContentHelper.convertToMap(XContentType.JSON.xContent(), lastLine, randomBoolean());
            assertThat(lineData.get("test.extension"), is("sample-spi-value"));
        }
    }
}
