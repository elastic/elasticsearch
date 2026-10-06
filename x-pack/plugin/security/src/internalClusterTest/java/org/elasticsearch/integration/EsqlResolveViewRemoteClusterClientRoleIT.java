/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

/**
 * Verifies that ES|QL view resolution does not raise a spurious security exception on nodes that
 * lack the {@code remote_cluster_client} role when the query contains CCS index patterns.
 */
@ESTestCase.WithoutEntitlements
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class EsqlResolveViewRemoteClusterClientRoleIT extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestEncryptionServicePlugin.class);
        plugins.add(EsqlWithTrialLicensePlugin.class);
        return plugins;
    }

    public void testCcsQueryWithViewsDoesNotProduceSecurityExceptionOnNoRoleNode() throws Exception {
        var authedClient = client().filterWithHeader(
            Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
        );

        // Register a view so ViewResolver does not short-circuit (it skips when cluster state has no views).
        assertAcked(
            authedClient.execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View("local-view", "FROM my-index | LIMIT 10"))
            ).actionGet(TEST_REQUEST_TIMEOUT)
        );

        // Start a node with only the data role (no remote_cluster_client).
        String nodeWithoutRole = internalCluster().startNode(NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE)));

        var nodeClient = internalCluster().client(nodeWithoutRole)
            .filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING)));

        // Call EsqlResolveViewAction directly on the data-only node with a CCS index pattern.
        var req = new EsqlResolveViewAction.Request(TEST_REQUEST_TIMEOUT, false);
        req.indices("remote*:logs-*");
        EsqlResolveViewAction.Response response = nodeClient.execute(EsqlResolveViewAction.TYPE, req).actionGet(TEST_REQUEST_TIMEOUT);
        assertNotNull(response.views());
        assertEquals(0, response.views().length);
    }

    public static class EsqlWithTrialLicensePlugin extends EsqlPlugin {
        @Override
        protected XPackLicenseState getLicenseState() {
            return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.TRIAL, true, null));
        }

        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            // prevent SPI-based data source plugin loading
        }
    }
}
