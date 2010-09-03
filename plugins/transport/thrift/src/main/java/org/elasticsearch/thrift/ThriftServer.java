/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.transport.BindTransportException;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class ThriftServer extends AbstractLifecycleComponent<ThriftServer> {

    final String type;

    final String port;

    final String bindHost;

    final String publishHost;

    private final NetworkService networkService;

    private final TransportNodesInfoAction nodesInfoAction;

    private final ThriftRestImpl client;

    private final TProtocolFactory protocolFactory;

    private volatile TServer server;

    private volatile int portNumber;

    @Inject public ThriftServer(Settings settings, NetworkService networkService, TransportNodesInfoAction nodesInfoAction, ThriftRestImpl client) {
        super(settings);
        this.client = client;
        this.networkService = networkService;
        this.nodesInfoAction = nodesInfoAction;
        this.type = componentSettings.get("type", "threadpool");
        this.port = componentSettings.get("port", "9500-9600");
        this.bindHost = componentSettings.get("bind_host");
        this.publishHost = componentSettings.get("publish_host");

        if (componentSettings.get("protocol", "binary").equals("compact")) {
            protocolFactory = new TCompactProtocol.Factory();
        } else {
            protocolFactory = new TBinaryProtocol.Factory();
        }
    }

    @Override protected void doStart() throws ElasticSearchException {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override public boolean onPortNumber(int portNumber) {
                ThriftServer.this.portNumber = portNumber;
                try {
                    Rest.Processor processor = new Rest.Processor(client);
                    if ("threadpool_framed".equals(type) || "threadpool".equals("threadpool")) {
                        TTransportFactory transportFactory;
                        if ("threadpool_framed".equals(type)) {
                            transportFactory = new TFramedTransport.Factory();
                        } else {
                            transportFactory = new TTransportFactory();
                        }
                        TServerTransport serverTransport = new TServerSocket(portNumber);
                        server = new TThreadPoolServer(processor, serverTransport, transportFactory, protocolFactory);
                    } else if ("nonblocking".equals(type) || "hsha".equals(type)) {
                        TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(portNumber);
                        TFramedTransport.Factory transportFactory = new TFramedTransport.Factory();
                        if ("nonblocking".equals(type)) {
                            server = new TNonblockingServer(processor, serverTransport, transportFactory, protocolFactory);
                        } else {
                            server = new THsHaServer(processor, serverTransport, transportFactory, protocolFactory);
                        }
                    }
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            }
        });
        if (!success) {
            throw new BindTransportException("Failed to bind to [" + port + "]", lastException.get());
        }
        logger.info("bound on port [{}]", portNumber);
        try {
            nodesInfoAction.putNodeAttribute("thrift_address", new InetSocketAddress(networkService.resolvePublishHostAddress(publishHost), portNumber).toString());
        } catch (Exception e) {
            // ignore
        }

        daemonThreadFactory(settings, "thrift_server").newThread(new Runnable() {
            @Override public void run() {
                server.serve();
            }
        }).start();
    }

    @Override protected void doStop() throws ElasticSearchException {
        nodesInfoAction.removeNodeAttribute("thrift_address");
        server.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
    }
}
