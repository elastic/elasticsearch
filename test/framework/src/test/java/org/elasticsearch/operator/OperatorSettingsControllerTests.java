/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.operator.action.OperatorClusterSettingsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

public class OperatorSettingsControllerTests extends ESTestCase {

    public void testOperatorController() throws IOException {
        OperatorSettingsController controller = new OperatorSettingsController();
        controller.initHandlers(List.of(new OperatorClusterSettingsAction()));

        String testJSON = """
            {
                "cluster" : {
                  "persistent" : {
                    "indices.recovery.max_bytes_per_sec" : "50mb"
                  },
                  "transient" : {
                    "cluster.routing.allocation.enable": "none"
                  }
                }
            }
            """;

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process(parser);
        }
    }
}
