/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

public class ESJsonLayoutTests extends ESTestCase {
    @BeforeClass
    public static void initNodeName() {
        assert "false".equals(System.getProperty("tests.security.manager")) : "-Dtests.security.manager=false has to be set";
        JsonLogsTestSetup.init();
    }

    public void testEmptyType() {
        expectThrows(IllegalArgumentException.class, () -> ESJsonLayout.newBuilder().build());
    }

    @SuppressForbidden(reason = "Need to test that a system property can be looked up in logs")
    public void testLayout() {
        System.setProperty("es.logs.cluster_name", "cluster123");
        ESJsonLayout server = ESJsonLayout.newBuilder().setType("server").build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();
        assertThat(conversionPattern, Matchers.equalTo(Strings.format("""
            {\
            "type": "server", \
            "timestamp": "%%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}", \
            "level": "%%p", \
            "component": "%%c{1.}", \
            "cluster.name": "${sys:es.logs.cluster_name}", \
            "node.name": "%%node_name", \
            "message": "%%notEmpty{%%enc{%%marker}{JSON} }%%enc{%%.-10000m}{JSON}"%%notEmpty{, \
            %%node_and_cluster_id }%%notEmpty{, %%CustomMapFields }%%exceptionAsJson \
            }%n""")));

        assertThat(server.toSerializable(new Log4jLogEvent()), Matchers.containsString("\"cluster.name\": \"cluster123\""));
    }

    public void testLayoutWithAdditionalFieldOverride() {
        ESJsonLayout server = ESJsonLayout.newBuilder().setType("server").setOverrideFields("message").build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        // message field is removed as is expected to be provided by a field from a message
        assertThat(conversionPattern, Matchers.equalTo(Strings.format("""
            {\
            "type": "server", \
            "timestamp": "%%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}", \
            "level": "%%p", \
            "component": "%%c{1.}", \
            "cluster.name": "${sys:es.logs.cluster_name}", \
            "node.name": "%%node_name"%%notEmpty{, %%node_and_cluster_id }%%notEmpty{, %%CustomMapFields }%%exceptionAsJson \
            }%n""")));
    }
}
