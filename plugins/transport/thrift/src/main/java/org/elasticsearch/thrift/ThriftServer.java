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
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class ThriftServer extends AbstractLifecycleComponent<ThriftServer> {

    final int frame;

    final String port;

    final String bindHost;

    final String publishHost;

    private final NetworkService networkService;

    private final NodeService nodeService;

    private final ThriftRestImpl client;

    private final TProtocolFactory protocolFactory;

    private volatile TServer server;

    private volatile int portNumber;

    @Inject public ThriftServer(Settings settings, NetworkService networkService, NodeService nodeService, ThriftRestImpl client) {
        super(settings);
        this.client = client;
        this.networkService = networkService;
        this.nodeService = nodeService;
        this.frame = (int) componentSettings.getAsBytesSize("frame", new ByteSizeValue(-1)).bytes();
        this.port = componentSettings.get("port", "9500-9600");
        this.bindHost = componentSettings.get("bind_host", settings.get("transport.bind_host", settings.get("transport.host")));
        this.publishHost = componentSettings.get("publish_host", settings.get("transport.publish_host", settings.get("transport.host")));

        if (componentSettings.get("protocol", "binary").equals("compact")) {
            protocolFactory = new TCompactProtocol.Factory();
        } else {
            protocolFactory = new TBinaryProtocol.Factory();
        }
    }

    @Override protected void doStart() throws ElasticSearchException {
        InetAddress bindAddrX;
        try {
            bindAddrX = networkService.resolveBindHostAddress(bindHost);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress bindAddr = bindAddrX;

        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override public boolean onPortNumber(int portNumber) {
                ThriftServer.this.portNumber = portNumber;
                try {
                    Rest.Processor processor = new Rest.Processor(client);

                    // Bind and start to accept incoming connections.
                    TServerSocket serverSocket = new TServerSocket(new InetSocketAddress(bindAddr, portNumber));

                    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket)
                            .minWorkerThreads(16)
                            .maxWorkerThreads(Integer.MAX_VALUE)
                            .inputProtocolFactory(protocolFactory)
                            .outputProtocolFactory(protocolFactory)
                            .processor(processor);

                    if (frame <= 0) {
                        args.inputTransportFactory(new TTransportFactory());
                        args.outputTransportFactory(new TTransportFactory());
                    } else {
                        args.inputTransportFactory(new TFramedTransport.Factory(frame));
                        args.outputTransportFactory(new TFramedTransport.Factory(frame));
                    }

                    server = new TThreadPoolServer(args);
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
            nodeService.putNodeAttribute("thrift_address", new InetSocketAddress(networkService.resolvePublishHostAddress(publishHost), portNumber).toString());
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
        nodeService.removeNodeAttribute("thrift_address");
        server.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
    }
}
