/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport.netty4.channel;

import io.netty.channel.socket.nio.NioSocketChannel;
import org.elasticsearch.SpecialPermission;

import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Wraps netty calls to {@link java.nio.channels.SocketChannel#connect(SocketAddress)} in
 * {@link AccessController#doPrivileged(PrivilegedAction)} blocks. This is necessary to limit
 * {@link java.net.SocketPermission} to the transport module.
 */
public class PrivilegedNioSocketChannel extends NioSocketChannel {

    @Override
    protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Boolean>) () -> super.doConnect(remoteAddress, localAddress));
        } catch (PrivilegedActionException e) {
            throw (Exception) e.getCause();
        }
    }
}
