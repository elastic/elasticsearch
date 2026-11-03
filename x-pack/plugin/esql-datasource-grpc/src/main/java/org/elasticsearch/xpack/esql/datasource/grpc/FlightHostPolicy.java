/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.Tuple;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * The hosts a node may open a Flight connection to. A dataset names its host in the resource, so without a limit anyone who can create
 * one can make every data node connect anywhere it can reach, and read the answer from the error. The limit is checked when a dataset
 * is registered and again whenever a client is built, so that narrowing the setting also stops datasets that already exist.
 * <p>
 * An entry is a host name (case-insensitive), a suffix pattern ({@code *.example.com}), an IP address, a CIDR range
 * ({@code 10.0.0.0/8}) or {@code *}, which allows every host. Names are compared as written and never resolved: an entry for a name
 * allows whatever that name resolves to, which is for the administrator who lists it to decide. A host given as an IP address matches
 * only the address and range entries, so a range cannot be bypassed by spelling the address as a name.
 */
final class FlightHostPolicy {

    /** Only this machine: what the demo, the tests and a satellite next to its node need, and nothing that can be reached remotely. */
    static final List<String> DEFAULT_ALLOWED = List.of("localhost", "127.0.0.0/8", "::1");

    static final FlightHostPolicy DEFAULT = new FlightHostPolicy(DEFAULT_ALLOWED);

    private final List<String> entries;
    private final List<Predicate<String>> names = new ArrayList<>();
    private final List<Predicate<InetAddress>> addresses = new ArrayList<>();
    private final boolean any;

    FlightHostPolicy(List<String> entries) {
        this.entries = List.copyOf(entries);
        boolean all = false;
        for (String entry : entries) {
            String e = entry.trim().toLowerCase(Locale.ROOT);
            if (e.isEmpty()) {
                throw new IllegalArgumentException("an allowed host must not be empty");
            }
            if (e.equals("*")) {
                all = true;
            } else if (e.indexOf('/') >= 0) {
                Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(e);
                addresses.add(address -> inRange(address, cidr.v1(), cidr.v2()));
            } else if (InetAddresses.isInetAddress(e)) {
                InetAddress expected = InetAddresses.forString(e);
                addresses.add(expected::equals);
            } else if (e.startsWith("*.") && e.length() > 2) {
                String suffix = e.substring(1);
                names.add(host -> host.endsWith(suffix));
            } else {
                names.add(e::equals);
            }
        }
        this.any = all;
    }

    private static boolean inRange(InetAddress address, InetAddress base, int prefixBits) {
        byte[] a = address.getAddress();
        byte[] b = base.getAddress();
        if (a.length != b.length) {
            return false;
        }
        for (int bit = 0; bit < prefixBits; bit++) {
            int mask = 0x80 >> (bit % 8);
            if ((a[bit / 8] & mask) != (b[bit / 8] & mask)) {
                return false;
            }
        }
        return true;
    }

    /** Throws if {@code host} (a name, an IP address, possibly an IPv6 address in brackets) may not be connected to. */
    void check(String host) {
        if (any) {
            return;
        }
        String h = host == null ? "" : host.toLowerCase(Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]")) {
            h = h.substring(1, h.length() - 1);
        }
        boolean allowed;
        if (InetAddresses.isInetAddress(h)) {
            InetAddress address = InetAddresses.forString(h);
            allowed = addresses.stream().anyMatch(p -> p.test(address));
        } else {
            String name = h;
            allowed = names.stream().anyMatch(p -> p.test(name));
        }
        if (allowed == false) {
            throw new IllegalArgumentException(
                "the Flight host ["
                    + host
                    + "] is not allowed; the nodes may connect to "
                    + entries
                    + " (setting ["
                    + GrpcDataSourcePlugin.FLIGHT_ALLOWED_HOSTS.getKey()
                    + "])"
            );
        }
    }
}
