/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent.api;

/**
 * A collector for user-agent parsed information.
 * Implementations can write parsed info directly into the collecting data structure.
 */
public interface UserAgentInfoCollector {

    void name(String name);

    void version(String version);

    void osName(String osName);

    void osVersion(String osVersion);

    void osFull(String osFull);

    void deviceName(String deviceName);

    void deviceVersion(String deviceVersion);

    void deviceType(String deviceType);
}
