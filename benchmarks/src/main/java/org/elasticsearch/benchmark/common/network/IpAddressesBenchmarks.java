/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.network;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(1)
public class IpAddressesBenchmarks {

    @Param("1000")
    private int size;
    private String[] ipV6Addresses;
    private String[] ipV4Addresses;
    private XContentString[] ipV6AddressesBytes;
    private XContentString[] ipV4AddressesBytes;

    @Setup
    public void setup() throws UnknownHostException {
        Random random = new Random();
        ipV6Addresses = new String[size];
        ipV4Addresses = new String[size];
        ipV6AddressesBytes = new XContentString[size];
        ipV4AddressesBytes = new XContentString[size];
        byte[] ipv6Bytes = new byte[16];
        byte[] ipv4Bytes = new byte[4];
        for (int i = 0; i < size; i++) {
            random.nextBytes(ipv6Bytes);
            random.nextBytes(ipv4Bytes);
            String ipv6String = InetAddresses.toAddrString(InetAddress.getByAddress(ipv6Bytes));
            String ipv4String = InetAddresses.toAddrString(InetAddress.getByAddress(ipv4Bytes));
            ipV6Addresses[i] = ipv6String;
            ipV4Addresses[i] = ipv4String;
            ipV6AddressesBytes[i] = new Text(ipv6String);
            ipV4AddressesBytes[i] = new Text(ipv4String);
        }
    }

    @Benchmark
    public boolean isInetAddressIpv6() {
        boolean b = true;
        for (int i = 0; i < size; i++) {
            b ^= InetAddresses.isInetAddress(ipV6Addresses[i]);
        }
        return b;
    }

    @Benchmark
    public boolean isInetAddressIpv4() {
        boolean b = true;
        for (int i = 0; i < size; i++) {
            b ^= InetAddresses.isInetAddress(ipV4Addresses[i]);
        }
        return b;
    }

    @Benchmark
    public void getIpOrHostIpv6(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.getIpOrHost(ipV6Addresses[i]));
        }
    }

    @Benchmark
    public void getIpOrHostIpv4(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.forString(ipV4Addresses[i]));
        }
    }

    @Benchmark
    public void forStringIpv6String(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.forString(ipV6Addresses[i]));
        }
    }

    @Benchmark
    public void forStringIpv4String(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.forString(ipV4Addresses[i]));
        }
    }

    @Benchmark
    public void forStringIpv6Bytes(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.forString(ipV6AddressesBytes[i].bytes()));
        }
    }

    @Benchmark
    public void forStringIpv4Bytes(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.forString(ipV4AddressesBytes[i].bytes()));
        }
    }

    @Benchmark
    public void encodeAsIpv6WithIpv6(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.encodeAsIpv6(ipV6AddressesBytes[i]));
        }
    }

    @Benchmark
    public void encodeAsIpv6WithIpv4(Blackhole blackhole) {
        for (int i = 0; i < size; i++) {
            blackhole.consume(InetAddresses.encodeAsIpv6(ipV4AddressesBytes[i]));
        }
    }
}
