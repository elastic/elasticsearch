/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SourceModeRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    public SourceModeRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testConfigureStoredSourceBeforeIndexCreationLegacy() throws IOException {
        assumeTrue("testing deprecation warnings and deprecation migrations", getOldClusterTestVersion().before("9.0.0"));
        String templateName = "logs@custom";
        if (isOldCluster()) {
            var storedSourceMapping = """
                {
                  "template": {
                    "settings": {
                      "index": {
                        "mode": "logsdb"
                      }
                    },
                    "mappings": {
                      "_source": {
                        "mode": "stored"
                      }
                    }
                  }
                }""";
            var putComponentTemplateRequest = new Request("PUT", "/_component_template/" + templateName);
            putComponentTemplateRequest.setOptions(expectWarnings(SourceFieldMapper.DEPRECATION_WARNING));
            putComponentTemplateRequest.setJsonEntity(storedSourceMapping);
            assertOK(client().performRequest(putComponentTemplateRequest));

            var request = new Request("GET", "/_migration/deprecations");
            var nodeSettings = (Map<?, ?>) ((List<?>) entityAsMap(client().performRequest(request)).get("node_settings")).getFirst();
            assertThat(nodeSettings.get("message"), equalTo(SourceFieldMapper.DEPRECATION_WARNING));
            assertThat(
                (String) nodeSettings.get("details"),
                containsString(SourceFieldMapper.DEPRECATION_WARNING + " Affected component templates: [" + templateName + "]")
            );
        } else if (isUpgradedCluster()) {
            var request = new Request("GET", "/_migration/deprecations");
            var nodeSettings = (Map<?, ?>) ((List<?>) entityAsMap(client().performRequest(request)).get("node_settings")).getFirst();
            assertThat(nodeSettings.get("message"), equalTo(SourceFieldMapper.DEPRECATION_WARNING));
            assertThat(
                (String) nodeSettings.get("details"),
                containsString(SourceFieldMapper.DEPRECATION_WARNING + " Affected component templates: [" + templateName + "]")
            );
        }
    }

    public void testConfigureStoredSourceWhenIndexIsCreatedLegacy() throws IOException {
        assumeTrue("testing deprecation warnings and deprecation migrations", getOldClusterTestVersion().before("9.0.0"));
        String templateName = "logs@custom";
        if (isOldCluster()) {
            var storedSourceMapping = """
                {
                  "template": {
                    "mappings": {
                      "_source": {
                        "mode": "stored"
                      }
                    }
                  }
                }""";
            var putComponentTemplateRequest = new Request("PUT", "/_component_template/" + templateName);
            putComponentTemplateRequest.setOptions(expectWarnings(SourceFieldMapper.DEPRECATION_WARNING));
            putComponentTemplateRequest.setJsonEntity(storedSourceMapping);
            assertOK(client().performRequest(putComponentTemplateRequest));

            var request = new Request("GET", "/_migration/deprecations");
            var nodeSettings = (Map<?, ?>) ((List<?>) entityAsMap(client().performRequest(request)).get("node_settings")).getFirst();
            assertThat(nodeSettings.get("message"), equalTo(SourceFieldMapper.DEPRECATION_WARNING));
            assertThat(
                (String) nodeSettings.get("details"),
                containsString(SourceFieldMapper.DEPRECATION_WARNING + " Affected component templates: [" + templateName + "]")
            );
        } else if (isUpgradedCluster()) {
            var request = new Request("GET", "/_migration/deprecations");
            var nodeSettings = (Map<?, ?>) ((List<?>) entityAsMap(client().performRequest(request)).get("node_settings")).getFirst();
            assertThat(nodeSettings.get("message"), equalTo(SourceFieldMapper.DEPRECATION_WARNING));
            assertThat(
                (String) nodeSettings.get("details"),
                containsString(SourceFieldMapper.DEPRECATION_WARNING + " Affected component templates: [" + templateName + "]")
            );
        }
    }
}
