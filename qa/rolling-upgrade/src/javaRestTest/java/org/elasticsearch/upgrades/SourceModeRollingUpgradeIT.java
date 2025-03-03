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
            assertDeprecationWarningForTemplate(templateName);
        } else if (isUpgradedCluster()) {
            assertDeprecationWarningForTemplate(templateName);
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
            assertDeprecationWarningForTemplate(templateName);
        } else if (isUpgradedCluster()) {
            assertDeprecationWarningForTemplate(templateName);
        }
    }

    private void assertDeprecationWarningForTemplate(String templateName) throws IOException {
        var request = new Request("GET", "/_migration/deprecations");
        var response = entityAsMap(client().performRequest(request));
        assertThat(response.containsKey("templates"), equalTo(true));
        Map<?, ?> issuesByTemplate = (Map<?, ?>) response.get("templates");
        assertThat(issuesByTemplate.containsKey(templateName), equalTo(true));
        var templateIssue = (Map<?, ?>) ((List<?>) issuesByTemplate.get(templateName)).getFirst();
        // Bwc compatible logic until backports are complete.
        if (templateIssue.containsKey("details")) {
            assertThat(templateIssue.get("message"), equalTo(SourceFieldMapper.DEPRECATION_WARNING_TITLE));
            assertThat(templateIssue.get("details"), equalTo(SourceFieldMapper.DEPRECATION_WARNING));
        } else {
            assertThat(templateIssue.get("message"), equalTo(SourceFieldMapper.DEPRECATION_WARNING));
        }
    }
}
