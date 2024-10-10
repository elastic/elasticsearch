/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class ILMHistoryManagedTemplateUpgradeIT extends AbstractUpgradeTestCase {

    @SuppressWarnings("unchecked")
    public void testEnsureHistoryManagedTemplateIsInstalledOnUpgradedVersion() throws Exception {
        if (CLUSTER_TYPE.equals(ClusterType.UPGRADED)) {
            assertBusy(() -> {
                Request request = new Request("GET", "/_index_template/ilm-history-7");
                try {
                    Response response = client().performRequest(request);
                    Map<String, Object> responseMap = entityAsMap(response);
                    assertNotNull(responseMap);

                    List<Map<String, Object>> indexTemplates = (List<Map<String, Object>>) responseMap.get("index_templates");
                    assertThat(indexTemplates.size(), is(1));
                    assertThat(ObjectPath.evaluate(indexTemplates.get(0), "name"), is("ilm-history-7"));
                    assertThat(ObjectPath.evaluate(indexTemplates.get(0), "index_template.index_patterns"), is(List.of("ilm-history-7*")));
                } catch (ResponseException e) {
                    // Not found is fine
                    assertThat(
                        "Unexpected failure getting templates: " + e.getResponse().getStatusLine(),
                        e.getResponse().getStatusLine().getStatusCode(),
                        is(404)
                    );
                }
            }, 30, TimeUnit.SECONDS);
        }
    }
}
