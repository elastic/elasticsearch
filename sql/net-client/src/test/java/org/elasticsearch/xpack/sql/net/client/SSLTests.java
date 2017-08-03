/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client;


import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

@AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/2074")
public class SSLTests extends ESTestCase {

    private static URL sslServer;

    @ClassRule
    public static ExternalResource SSL_SERVER = new ExternalResource() {
        private BasicSSLServer server;

        @Override
        protected void before() throws Throwable {
            server = new BasicSSLServer();
            server.start(0);

            sslServer = new URL(server.url());
        }

        @Override
        protected void after() {
            sslServer = null;
            try {
                server.stop();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    };

    private ConnectionConfiguration cfg;

    @Before
    public void setup() throws Exception {
        Properties prop = new Properties();
        // ssl config
        prop.setProperty("ssl", "true");
        // specify the TLS just in case (who knows what else will be deprecated across JDKs)
        prop.setProperty("ssl.protocol", "TLSv1.2");
        prop.setProperty("ssl.keystore.location",
                PathUtils.get(getClass().getResource("/ssl/client.keystore").toURI()).toRealPath().toString());
        prop.setProperty("ssl.keystore.pass", "password");
        // set the truststore as well since otherwise there will be cert errors ...
        prop.setProperty("ssl.truststore.location",
                PathUtils.get(getClass().getResource("/ssl/client.keystore").toURI()).toRealPath().toString());
        prop.setProperty("ssl.truststore.pass", "password");
        //prop.setProperty("ssl.accept.self.signed.certs", "true");
        
        cfg = new ConnectionConfiguration(prop);
    }

    @After
    public void destroy() {
        cfg = null;
    }

    public void testSslSetup() throws Exception {
        SSLContext context = SSLContext.getDefault();
        SSLSocketFactory factory = context.getSocketFactory();
        SSLSocket socket = (SSLSocket) factory.createSocket();

        String[] protocols = socket.getSupportedProtocols();

        logger.info("Supported Protocols: {}", protocols.length);
        logger.info("{}", Arrays.toString(protocols));

        protocols = socket.getEnabledProtocols();

        logger.info("Enabled Protocols: {}", protocols.length);
        logger.info("{}", Arrays.toString(protocols));

        String[] ciphers = socket.getSupportedCipherSuites();
        logger.info("{}", Arrays.toString(ciphers));
    }

    public void testSslHead() throws Exception {
        assertTrue(JreHttpUrlConnection.http(sslServer, cfg, JreHttpUrlConnection::head));
    }

    public void testSslPost() throws Exception {
        String message = UUID.randomUUID().toString();
        Bytes b = JreHttpUrlConnection.http(sslServer, cfg, c -> {
            return c.post(o -> {
                o.writeUTF(message);
            });
        });
        
        String received = new DataInputStream(new ByteArrayInputStream(b.bytes())).readUTF();
        assertEquals(message, received);
    }
}