/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class GetInferenceDiagnosticsActionNodeResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInferenceDiagnosticsAction.NodeResponse> {

    private static final TransportVersion ML_INFERENCE_ENDPOINT_CACHE = TransportVersion.fromName("ml_inference_endpoint_cache");
    private static final TransportVersion INFERENCE_API_EIS_DIAGNOSTICS = TransportVersion.fromName("inference_api_eis_diagnostics");
    private static final TransportVersion INFERENCE_OAUTH2_TOKEN_CACHE_DIAGNOSTICS = TransportVersion.fromName(
        "inference_api_oauth2_token_cache_diagnostics"
    );

    public static GetInferenceDiagnosticsAction.NodeResponse createRandom() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        return new GetInferenceDiagnosticsAction.NodeResponse(
            node,
            randomConnectionPoolStats(),
            randomConnectionPoolStats(),
            randomNullableCacheStats(),
            randomNullableCacheStats()
        );
    }

    @Override
    protected Writeable.Reader<GetInferenceDiagnosticsAction.NodeResponse> instanceReader() {
        return GetInferenceDiagnosticsAction.NodeResponse::new;
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse mutateInstance(GetInferenceDiagnosticsAction.NodeResponse instance)
        throws IOException {
        var externalPoolStats = instance.getExternalConnectionPoolStats();
        var eisPoolStats = instance.getEisMtlsConnectionPoolStats();
        var registryStats = instance.getInferenceEndpointRegistryStats();
        var oAuth2Stats = instance.getOauth2TokenCacheStats();

        switch (randomInt(3)) {
            case 0 -> externalPoolStats = randomValueOtherThan(
                externalPoolStats,
                GetInferenceDiagnosticsActionNodeResponseTests::randomConnectionPoolStats
            );
            case 1 -> eisPoolStats = randomValueOtherThan(
                eisPoolStats,
                GetInferenceDiagnosticsActionNodeResponseTests::randomConnectionPoolStats
            );
            case 2 -> registryStats = randomValueOtherThan(
                registryStats,
                GetInferenceDiagnosticsActionNodeResponseTests::randomNullableCacheStats
            );
            case 3 -> oAuth2Stats = randomValueOtherThan(
                oAuth2Stats,
                GetInferenceDiagnosticsActionNodeResponseTests::randomNullableCacheStats
            );
            default -> throw new AssertionError("Illegal randomization branch");
        }

        return new GetInferenceDiagnosticsAction.NodeResponse(
            instance.getNode(),
            externalPoolStats,
            eisPoolStats,
            registryStats,
            oAuth2Stats
        );
    }

    private static GetInferenceDiagnosticsAction.NodeResponse.ConnectionPoolStats randomConnectionPoolStats() {
        return GetInferenceDiagnosticsAction.NodeResponse.ConnectionPoolStats.of(randomInt(), randomInt(), randomInt(), randomInt());
    }

    private static GetInferenceDiagnosticsAction.NodeResponse.Stats randomNullableCacheStats() {
        return randomBoolean() ? null : randomCacheStats();
    }

    private static GetInferenceDiagnosticsAction.NodeResponse.Stats randomCacheStats() {
        return new GetInferenceDiagnosticsAction.NodeResponse.Stats(
            randomInt(),
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE)
        );
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse mutateInstanceForVersion(
        GetInferenceDiagnosticsAction.NodeResponse instance,
        TransportVersion version
    ) {
        return mutateNodeResponseForVersion(instance, version);
    }

    public static GetInferenceDiagnosticsAction.NodeResponse mutateNodeResponseForVersion(
        GetInferenceDiagnosticsAction.NodeResponse instance,
        TransportVersion version
    ) {
        if (version.supports(INFERENCE_OAUTH2_TOKEN_CACHE_DIAGNOSTICS)) {
            return instance;
        }

        var eis = instance.getEisMtlsConnectionPoolStats();
        var eisMtlsConnectionPoolStats = version.supports(INFERENCE_API_EIS_DIAGNOSTICS)
            ? GetInferenceDiagnosticsAction.NodeResponse.ConnectionPoolStats.of(
                eis.getLeasedConnections(),
                eis.getPendingConnections(),
                eis.getAvailableConnections(),
                eis.getMaxConnections()
            )
            : GetInferenceDiagnosticsAction.NodeResponse.ConnectionPoolStats.of(0, 0, 0, 0);

        var inferenceEndpointRegistryStats = version.supports(ML_INFERENCE_ENDPOINT_CACHE)
            ? instance.getInferenceEndpointRegistryStats()
            : null;

        var ext = instance.getExternalConnectionPoolStats();
        return new GetInferenceDiagnosticsAction.NodeResponse(
            instance.getNode(),
            GetInferenceDiagnosticsAction.NodeResponse.ConnectionPoolStats.of(
                ext.getLeasedConnections(),
                ext.getPendingConnections(),
                ext.getAvailableConnections(),
                ext.getMaxConnections()
            ),
            eisMtlsConnectionPoolStats,
            inferenceEndpointRegistryStats,
            null
        );
    }
}
