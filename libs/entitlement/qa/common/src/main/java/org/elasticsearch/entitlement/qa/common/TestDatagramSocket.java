/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.common;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.DatagramSocketImpl;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;

public class TestDatagramSocket extends DatagramSocket {
    public TestDatagramSocket() throws SocketException {
        super(new DatagramSocketImpl() {
            @Override
            protected void create() throws SocketException {

            }

            @Override
            protected void bind(int lport, InetAddress laddr) throws SocketException {

            }

            @Override
            protected void send(DatagramPacket p) throws IOException {

            }

            @Override
            protected int peek(InetAddress i) throws IOException {
                return 0;
            }

            @Override
            protected int peekData(DatagramPacket p) throws IOException {
                return 0;
            }

            @Override
            protected void receive(DatagramPacket p) throws IOException {

            }

            @Override
            protected void setTTL(byte ttl) throws IOException {

            }

            @Override
            protected byte getTTL() throws IOException {
                return 0;
            }

            @Override
            protected void setTimeToLive(int ttl) throws IOException {

            }

            @Override
            protected int getTimeToLive() throws IOException {
                return 0;
            }

            @Override
            protected void join(InetAddress inetaddr) throws IOException {

            }

            @Override
            protected void leave(InetAddress inetaddr) throws IOException {

            }

            @Override
            protected void joinGroup(SocketAddress mcastaddr, NetworkInterface netIf) throws IOException {

            }

            @Override
            protected void leaveGroup(SocketAddress mcastaddr, NetworkInterface netIf) throws IOException {

            }

            @Override
            protected void close() {}

            @Override
            public void setOption(int optID, Object value) throws SocketException {

            }

            @Override
            public Object getOption(int optID) throws SocketException {
                return null;
            }

            @Override
            protected void connect(InetAddress address, int port) throws SocketException {}
        });
    }
}
