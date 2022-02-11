/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

/**
 * This class defines common component names for the health indicators.
 */
public class ServerHealthComponents {

    public static final String CLUSTER_COORDINATION = "cluster_coordination";
    public static final String DATA = "data";
    public static final String SNAPSHOT = "snapshot";

    private ServerHealthComponents() {}
}
