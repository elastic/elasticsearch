/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Validates that the {@code _terms_enum} API accepts a {@code project_routing} parameter when the node runs in
 * cross-project-search mode ({@code serverless.cross_project.enabled} is on). With cross-project mode enabled,
 * {@link org.elasticsearch.xpack.core.termsenum.rest.RestTermsEnumAction} flips the request's indices options to resolve a
 * cross-project index expression, so {@link org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest#validate()}
 * passes and the request executes locally, returning the indexed terms.
 * <p>
 * This test validates only that {@code project_routing} is <em>accepted</em> and that the request <em>executes locally</em>
 * under cross-project-enabled mode. It does NOT exercise true multi-project routing, which is performed only by the
 * closed-source serverless {@code ProjectRoutingResolver} (absent here, a no-op in this repository) — the same limitation
 * under which the transform/ml cross-project-search integration tests operate.
 */
@ESIntegTestCase.ClusterScope(
    scope = ESIntegTestCase.Scope.TEST,
    supportsDedicatedMasters = false,
    numDataNodes = 1,
    numClientNodes = 0
)
public class TermsEnumProjectRoutingIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    /**
     * Registers the {@code serverless.cross_project.enabled} node setting, which only exists in the closed-source serverless
     * distribution. Copied verbatim from the transform/ml cross-project-search integration tests.
     */
    public static class CpsPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(Setting.boolSetting("serverless.cross_project.enabled", false, Setting.Property.NodeScope));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(LocalStateCompositeXPackPlugin.class);
        plugins.add(CpsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("serverless.cross_project.enabled", true)
            .build();
    }

    public void testProjectRoutingAcceptedAndExecutesLocally() throws IOException {
        String index = "test";
        assertAcked(indicesAdmin().prepareCreate(index).setMapping("foo", "type=keyword"));
        prepareIndex(index).setSource("foo", "foo").get();
        prepareIndex(index).setSource("foo", "foobar").get();
        indicesAdmin().prepareRefresh(index).get();

        Request request = new Request("POST", "/" + index + "/_terms_enum");
        request.setJsonEntity("""
            {
              "field": "foo",
              "project_routing": "_alias:_origin"
            }
            """);

        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        List<String> terms = objectPath.evaluate("terms");
        assertThat(terms, containsInAnyOrder("foo", "foobar"));
    }
}
