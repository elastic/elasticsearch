/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

import java.net.InetAddress;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LicenseTLSTests extends AbstractLicenseServiceTestCase {

    private InetAddress inetAddress;

    public void testApplyLicenseInDevMode() throws Exception {
        License newLicense = TestUtils.generateSignedLicense(randomFrom("gold", "platinum"), TimeValue.timeValueHours(24L));
        PutLicenseRequest request = new PutLicenseRequest();
        request.acknowledge(true);
        request.license(newLicense);
        Settings settings = Settings.builder().put("xpack.security.enabled", true).build();
        XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        inetAddress = InetAddress.getLoopbackAddress();

        setInitialState(null, licenseState, settings);
        licenseService.start();
        PlainActionFuture<PutLicenseResponse> responseFuture = new PlainActionFuture<>();
        licenseService.registerLicense(request, responseFuture);
        verify(clusterService).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class), any());

        inetAddress = TransportAddress.META_ADDRESS;
        settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put(DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();
        licenseService.stop();
        licenseState = new XPackLicenseState(() -> 0);
        setInitialState(null, licenseState, settings);
        licenseService.start();
        licenseService.registerLicense(request, responseFuture);
        verify(clusterService, times(2)).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class), any());
    }

    @Override
    protected DiscoveryNode getLocalNode() {
        return new DiscoveryNode(
            "localnode",
            new TransportAddress(inetAddress, randomIntBetween(9300, 9399)),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
    }
}
