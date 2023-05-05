/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.main;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.Date;

public class MainTransportResponseTests extends AbstractWireSerializingTestCase<MainTransportResponse> {

    @Override
    protected MainTransportResponse createTestInstance() {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Build build = new Build(Build.Type.UNKNOWN, randomAlphaOfLength(8), date, randomBoolean(), version.toString());
        return new MainTransportResponse(nodeName, version, clusterName, clusterUuid, build);
    }

    @Override
    protected Writeable.Reader<MainTransportResponse> instanceReader() {
        return MainTransportResponse::new;
    }

    @Override
    protected MainTransportResponse mutateInstance(MainTransportResponse mutateInstance) {
        String clusterUuid = mutateInstance.getClusterUuid();
        Build build = mutateInstance.getBuild();
        Version version = mutateInstance.getVersion();
        String nodeName = mutateInstance.getNodeName();
        ClusterName clusterName = mutateInstance.getClusterName();
        switch (randomIntBetween(0, 4)) {
            case 0 -> clusterUuid = clusterUuid + randomAlphaOfLength(5);
            case 1 -> nodeName = nodeName + randomAlphaOfLength(5);
            case 2 ->
                // toggle the snapshot flag of the original Build parameter
                build = new Build(Build.Type.UNKNOWN, build.hash(), build.date(), build.isSnapshot() == false, build.qualifiedVersion());
            case 3 -> version = randomValueOtherThan(version, () -> VersionUtils.randomVersion(random()));
            case 4 -> clusterName = new ClusterName(clusterName + randomAlphaOfLength(5));
        }
        return new MainTransportResponse(nodeName, version, clusterName, clusterUuid, build);
    }
}
