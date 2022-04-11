/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;

import org.elasticsearch.logging.locator.ServerSupportLocator;

/**
 * An SPI that has to be implemented by ES server in order provided the logging infra with the information
 * not available in logging framework
 */
public interface ServerSupport {
    ServerSupport INSTANCE = ServerSupportLocator.INSTANCE;

    byte[] quoteAsUTF8(String line);

    /** Return a tuple, where the first element is the node name, and second is the cluster Id (in string form). */
    String nodeId();

    String clusterId();

    // Header Warning support
    void addHeaderWarning(String message, Object... params);

    // TODO: warning header from where, context? improve docs
    String getXOpaqueIdHeader();

    String getProductOriginHeader();

    String getTraceIdHeader();

    // settings

    String getClusterNameSettingValue();

    String getNodeNameSettingValue();

}
