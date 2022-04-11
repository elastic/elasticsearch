/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.NodeAndClusterIdStateListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.spi.ServerSupport;
import org.elasticsearch.xcontent.json.JsonStringEncoder;

/* SPI for logging support. */
public class ServerSupportImpl implements ServerSupport {

    private static Settings settings;

    // -- Header Warning
    @Override
    public void addHeaderWarning(String message, Object... params) {
        HeaderWarning.addWarning(message, params);
    }

    @Override
    public String getXOpaqueIdHeader() {
        return HeaderWarning.getXOpaqueId();
    }

    @Override
    public String getProductOriginHeader() {
        return HeaderWarning.getProductOrigin();
    }

    @Override
    public String getTraceIdHeader() {
        return HeaderWarning.getTraceId();
    }

    // --

    // TODO PG not ideal.. maybe we can have some similar impl in some util?
    @Override
    public byte[] quoteAsUTF8(String line) {
        return JsonStringEncoder.getInstance().quoteAsUTF8(line);
    }

    @Override
    public String nodeId() {
        Tuple<String, String> nodeIdAndClusterId = NodeAndClusterIdStateListener.getNodeIdAndClusterId();
        return nodeIdAndClusterId != null ? nodeIdAndClusterId.v1() : null;
    }

    @Override
    public String clusterId() {
        Tuple<String, String> nodeIdAndClusterId = NodeAndClusterIdStateListener.getNodeIdAndClusterId();
        return nodeIdAndClusterId != null ? nodeIdAndClusterId.v2() : null;
    }
    // -- settings

    @Override
    public String getClusterNameSettingValue() {
        return ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        // Node.NODE_NAME_SETTING.get(settings));
    }

    @Override
    public String getNodeNameSettingValue() {
        return null;
    }
}
