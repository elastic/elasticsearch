/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Validates that the {@code _terms_enum} API rejects a {@code project_routing} parameter when the node is not running in
 * cross-project-search mode ({@code serverless.cross_project.enabled} is off). In that case
 * {@link org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest#validate()} returns a validation error because the
 * request's indices options do not resolve a cross-project index expression.
 */
@ESIntegTestCase.ClusterScope(
    scope = ESIntegTestCase.Scope.TEST,
    supportsDedicatedMasters = false,
    numDataNodes = 1,
    numClientNodes = 0
)
public class TermsEnumProjectRoutingDisallowedInNonCpsEnvIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    public void testDisallowProjectRouting() throws IOException {
        String index = "test";
        assertAcked(indicesAdmin().prepareCreate(index).setMapping("foo", "type=keyword"));
        prepareIndex(index).setSource("foo", "foo").get();
        indicesAdmin().prepareRefresh(index).get();

        Request request = new Request("POST", "/" + index + "/_terms_enum");
        request.setJsonEntity("""
            {
              "field": "foo",
              "project_routing": "_alias:_origin"
            }
            """);

        ResponseException err = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(err.toString(), Matchers.containsString("Unknown key for a VALUE_STRING in [project_routing]"));
    }
}
