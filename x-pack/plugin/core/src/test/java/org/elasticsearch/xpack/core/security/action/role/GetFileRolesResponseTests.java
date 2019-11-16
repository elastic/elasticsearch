/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GetFileRolesResponseTests extends AbstractWireSerializingTestCase<GetFileRolesResponse> {

    @Override
    protected GetFileRolesResponse createTestInstance() {
        List<GetFileRolesAction.NodeResponse> responses = Arrays.asList(
            randomArray(1, 10, GetFileRolesAction.NodeResponse[]::new, GetFileRolesResponseTests::randomNodeResponse));
        return new GetFileRolesResponse(new ClusterName(randomAlphaOfLength(10)), responses, Collections.emptyList());
    }

    @Override
    protected Writeable.Reader<GetFileRolesResponse> instanceReader() {
        return GetFileRolesResponse::new;
    }

    @Override
    protected GetFileRolesResponse mutateInstance(GetFileRolesResponse instance) {
        int mutate = randomIntBetween(1,2);
        switch (mutate) {
            case 1:
                String newClusterName = randomValueOtherThan(instance.getClusterName().value(), () -> randomAlphaOfLength(10));
                return new GetFileRolesResponse(new ClusterName(newClusterName), instance.getNodes(), instance.failures());
            case 2:
                List<GetFileRolesAction.NodeResponse> newResponses = randomValueOtherThan(instance.getNodes(), () -> Arrays.asList(
                    randomArray(1, 10, GetFileRolesAction.NodeResponse[]::new, GetFileRolesResponseTests::randomNodeResponse)));
                return new GetFileRolesResponse(instance.getClusterName(), newResponses, instance.failures());
            default:
                fail("invalid randomization");
        }
        throw new IllegalArgumentException("invalid randomization, did not return from switch");
    }

    private static DiscoveryNode randomDiscoveryNode() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress(randomAlphaOfLength(5),
            new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

        return new DiscoveryNode(randomAlphaOfLength(5), randomAlphaOfLength(5), transportAddress,
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
    }

    private static RoleDescriptor randomRoleDescriptor() {
        String[] clusterPrivs = randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(5, 15));
        return new RoleDescriptor(randomAlphaOfLengthBetween(5, 15), clusterPrivs, null, null);
    }

    private static GetFileRolesAction.NodeResponse randomNodeResponse() {
        DiscoveryNode node;
        try {
            node = randomDiscoveryNode();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<RoleDescriptor> roles = Arrays.asList(
            randomArray(0, 10, RoleDescriptor[]::new, GetFileRolesResponseTests::randomRoleDescriptor));
        return new GetFileRolesAction.NodeResponse(node, roles);
    }
}
