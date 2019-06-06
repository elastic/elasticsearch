/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.common.network;

import java.io.IOException;
import java.net.InetAddress;

/**
 * We use this class to access the package private method in NetworkUtils to resolve anyLocalAddress InetAddresses for certificate
 * generation
 */
public class InetAddressHelper {

    private InetAddressHelper() {}

    public static InetAddress[] getAllAddresses() throws IOException {
        return NetworkUtils.getAllAddresses();
    }

    public static InetAddress[] filterIPV4(InetAddress[] addresses){
        return NetworkUtils.filterIPV4(addresses);
    }

    public static InetAddress[] filterIPV6(InetAddress[] addresses){
        return NetworkUtils.filterIPV6(addresses);
    }
}
