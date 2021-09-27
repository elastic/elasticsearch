/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.ssl.StoreKeyConfig;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BaseEnrollmentTokenGenerator {

    public BaseEnrollmentTokenGenerator() {
    }

    static String getCaFingerprint(SSLService sslService) throws Exception {
        final SslKeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().getKeyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration is " +
                "not configured with a keystore");
        }
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
            ((StoreKeyConfig) keyConfig).getKeys().stream()
                .filter(t -> t.v2().getBasicConstraints() != -1)
                .collect(Collectors.toList());
        if (httpCaKeysAndCertificates.isEmpty()) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore doesn't contain any PrivateKey entries where the associated certificate is a CA certificate");
        } else if (httpCaKeysAndCertificates.size() > 1) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore contains multiple PrivateKey entries where the associated certificate is a CA certificate");
        }
        return SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2(), "SHA-256");
    }

    static List<String> getFilteredAddresses(List<String> addresses) throws Exception {
        List<String> filteredAddresses = new ArrayList<>();
        for (String boundAddress : addresses){
            InetAddress inetAddress = getInetAddressFromString(boundAddress);
            if (inetAddress.isLoopbackAddress() != true) {
                filteredAddresses.add(boundAddress);
            }
        }
        if (filteredAddresses.isEmpty()) {
            filteredAddresses = addresses;
        }
        // Sort the list prioritizing IPv4 addresses when possible, as it is more probable to be reachable when token consumer iterates
        // addresses for the initial node and it is less surprising for users to see in the UI or config
        filteredAddresses.sort((String a, String b) -> {
            try {
                final InetAddress addressA = getInetAddressFromString(a);
                final InetAddress addressB = getInetAddressFromString(b);
                if (addressA instanceof Inet4Address && addressB instanceof Inet6Address) {
                    return -1;
                } else if (addressA instanceof Inet6Address && addressB instanceof Inet4Address) {
                    return 1;
                } else {
                    return 0;
                }
            } catch (Exception e) {
                return 0;
            }
        });
        return filteredAddresses;
    }

    private static InetAddress getInetAddressFromString(String address) throws Exception {
        URI uri = new URI("http://" + address);
        return InetAddress.getByName(uri.getHost());
    }
}
