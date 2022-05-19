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
import java.util.Comparator;
import java.util.List;

public class BaseEnrollmentTokenGenerator {
    public static final long ENROLL_API_KEY_EXPIRATION_MINUTES = 30L;

    public BaseEnrollmentTokenGenerator() {}

    static String getHttpsCaFingerprint(SSLService sslService) throws Exception {
        final SslKeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            throw new IllegalStateException(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration is "
                    + "not configured with a keystore"
            );
        }
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates = keyConfig.getKeys()
            .stream()
            .filter(t -> t.v2().getBasicConstraints() != -1)
            .toList();
        if (httpCaKeysAndCertificates.isEmpty()) {
            throw new IllegalStateException(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration "
                    + "Keystore doesn't contain any PrivateKey entries where the associated certificate is a CA certificate"
            );
        } else if (httpCaKeysAndCertificates.size() > 1) {
            throw new IllegalStateException(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration "
                    + "Keystore contains multiple PrivateKey entries where the associated certificate is a CA certificate"
            );
        }
        return SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2(), "SHA-256");
    }

    static Tuple<List<String>, List<String>> splitAddresses(List<String> addresses) throws Exception {
        final List<String> nonLocalAddresses = new ArrayList<>();
        final List<String> localAddresses = new ArrayList<>();
        for (String boundAddress : addresses) {
            InetAddress inetAddress = getInetAddressFromString(boundAddress);
            if (inetAddress.isLoopbackAddress()) {
                localAddresses.add(boundAddress);
            } else if (inetAddress.isAnyLocalAddress() == false) {
                nonLocalAddresses.add(boundAddress);
            }
        }
        // Sort the list prioritizing IPv4 addresses when possible, as it is more probable to be reachable when token consumer iterates
        // addresses for the initial node and it is less surprising for users to see in the UI or config
        final Comparator<String> ipv4BeforeIpv6Comparator = (String a, String b) -> {
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
        };
        localAddresses.sort(ipv4BeforeIpv6Comparator);
        nonLocalAddresses.sort(ipv4BeforeIpv6Comparator);
        final List<String> distinctLocalAddresses = localAddresses.stream().distinct().toList();
        final List<String> distinctNonLocalAddresses = nonLocalAddresses.stream().distinct().toList();
        return new Tuple<>(distinctLocalAddresses, distinctNonLocalAddresses);
    }

    static List<String> getFilteredAddresses(Tuple<List<String>, List<String>> splitAddresses) {
        // If there are no non-local addresses, the enrollment token contains only local addresses
        if (splitAddresses.v2().isEmpty()) {
            return splitAddresses.v1();
        } else {
            // otherwise it contains only non-local addresses
            return splitAddresses.v2();
        }
    }

    static List<String> getFilteredAddresses(List<String> addresses) throws Exception {
        Tuple<List<String>, List<String>> splitAddresses = splitAddresses(addresses);
        return getFilteredAddresses(splitAddresses);
    }

    static String getIpFromPublishAddress(String publishAddress) {
        if (publishAddress.contains("/")) {
            return publishAddress.split("/")[1];
        }
        return publishAddress;
    }

    private static InetAddress getInetAddressFromString(String address) throws Exception {
        URI uri = new URI("http://" + address);
        return InetAddress.getByName(uri.getHost());
    }
}
