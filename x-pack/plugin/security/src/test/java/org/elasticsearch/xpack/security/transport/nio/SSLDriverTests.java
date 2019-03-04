/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class SSLDriverTests extends ESTestCase {

    private final Supplier<InboundChannelBuffer.Page> pageSupplier =
            () -> new InboundChannelBuffer.Page(ByteBuffer.allocate(1 << 14), () -> {});
    private InboundChannelBuffer serverBuffer = new InboundChannelBuffer(pageSupplier);
    private InboundChannelBuffer clientBuffer = new InboundChannelBuffer(pageSupplier);
    private InboundChannelBuffer genericBuffer = new InboundChannelBuffer(pageSupplier);

    public void testPingPongAndClose() throws Exception {
        SSLContext sslContext = getSSLContext();

        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        handshake(clientDriver, serverDriver);

        ByteBuffer[] buffers = {ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))};
        sendAppData(clientDriver, serverDriver, buffers);
        serverDriver.read(serverBuffer);
        assertEquals(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8)), serverBuffer.sliceBuffersTo(4)[0]);

        ByteBuffer[] buffers2 = {ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8))};
        sendAppData(serverDriver, clientDriver, buffers2);
        clientDriver.read(clientBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), clientBuffer.sliceBuffersTo(4)[0]);

        assertFalse(clientDriver.needsNonApplicationWrite());
        normalClose(clientDriver, serverDriver);
    }

    public void testRenegotiate() throws Exception {
        SSLContext sslContext = getSSLContext();

        SSLEngine serverEngine = sslContext.createSSLEngine();
        SSLEngine clientEngine = sslContext.createSSLEngine();

        String[] serverProtocols = {"TLSv1.2"};
        serverEngine.setEnabledProtocols(serverProtocols);
        String[] clientProtocols = {"TLSv1.2"};
        clientEngine.setEnabledProtocols(clientProtocols);
        SSLDriver clientDriver = getDriver(clientEngine, true);
        SSLDriver serverDriver = getDriver(serverEngine, false);

        handshake(clientDriver, serverDriver);

        ByteBuffer[] buffers = {ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))};
        sendAppData(clientDriver, serverDriver, buffers);
        serverDriver.read(serverBuffer);
        assertEquals(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8)), serverBuffer.sliceBuffersTo(4)[0]);

        clientDriver.renegotiate();
        assertTrue(clientDriver.isHandshaking());
        assertFalse(clientDriver.readyForApplicationWrites());

        // This tests that the client driver can still receive data based on the prior handshake
        ByteBuffer[] buffers2 = {ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8))};
        sendAppData(serverDriver, clientDriver, buffers2);
        clientDriver.read(clientBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), clientBuffer.sliceBuffersTo(4)[0]);

        handshake(clientDriver, serverDriver, true);
        sendAppData(clientDriver, serverDriver, buffers);
        serverDriver.read(serverBuffer);
        assertEquals(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8)), serverBuffer.sliceBuffersTo(4)[0]);
        sendAppData(serverDriver, clientDriver, buffers2);
        clientDriver.read(clientBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), clientBuffer.sliceBuffersTo(4)[0]);

        normalClose(clientDriver, serverDriver);
    }

    public void testBigApplicationData() throws Exception {
        SSLContext sslContext = getSSLContext();

        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        handshake(clientDriver, serverDriver);

        ByteBuffer buffer = ByteBuffer.allocate(1 << 15);
        for (int i = 0; i < (1 << 15); ++i) {
            buffer.put((byte) i);
        }
        ByteBuffer[] buffers = {buffer};
        sendAppData(clientDriver, serverDriver, buffers);
        serverDriver.read(serverBuffer);
        assertEquals(16384, serverBuffer.sliceBuffersFrom(0)[0].limit());
        assertEquals(16384, serverBuffer.sliceBuffersFrom(0)[1].limit());

        ByteBuffer[] buffers2 = {ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8))};
        sendAppData(serverDriver, clientDriver, buffers2);
        clientDriver.read(clientBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), clientBuffer.sliceBuffersTo(4)[0]);

        assertFalse(clientDriver.needsNonApplicationWrite());
        normalClose(clientDriver, serverDriver);
    }

    public void testHandshakeFailureBecauseProtocolMismatch() throws Exception {
        SSLContext sslContext = getSSLContext();
        SSLEngine clientEngine = sslContext.createSSLEngine();
        SSLEngine serverEngine = sslContext.createSSLEngine();
        String[] serverProtocols = {"TLSv1.2"};
        serverEngine.setEnabledProtocols(serverProtocols);
        String[] clientProtocols = {"TLSv1.1"};
        clientEngine.setEnabledProtocols(clientProtocols);
        SSLDriver clientDriver = getDriver(clientEngine, true);
        SSLDriver serverDriver = getDriver(serverEngine, false);

        SSLException sslException = expectThrows(SSLException.class, () -> handshake(clientDriver, serverDriver));
        String oldExpected = "Client requested protocol TLSv1.1 not enabled or not supported";
        String jdk11Expected = "The client supported protocol versions [TLSv1.1] are not accepted by server preferences [TLS12]";
        boolean expectedMessage = oldExpected.equals(sslException.getMessage()) || jdk11Expected.equals(sslException.getMessage());
        assertTrue("Unexpected exception message: " + sslException.getMessage(), expectedMessage);

        // In JDK11 we need an non-application write
        if (serverDriver.needsNonApplicationWrite()) {
            serverDriver.nonApplicationWrite();
        }
        // Prior to JDK11 we still need to send a close alert
        if (serverDriver.isClosed() == false) {
            failedCloseAlert(serverDriver, clientDriver, Arrays.asList("Received fatal alert: protocol_version",
                "Received fatal alert: handshake_failure"));
        }
    }

    public void testHandshakeFailureBecauseNoCiphers() throws Exception {
        SSLContext sslContext = getSSLContext();
        SSLEngine clientEngine = sslContext.createSSLEngine();
        SSLEngine serverEngine = sslContext.createSSLEngine();
        String[] enabledCipherSuites = clientEngine.getEnabledCipherSuites();
        int midpoint = enabledCipherSuites.length / 2;
        String[] serverCiphers = Arrays.copyOfRange(enabledCipherSuites, 0, midpoint);
        serverEngine.setEnabledCipherSuites(serverCiphers);
        String[] clientCiphers = Arrays.copyOfRange(enabledCipherSuites, midpoint, enabledCipherSuites.length - 1);
        clientEngine.setEnabledCipherSuites(clientCiphers);
        SSLDriver clientDriver = getDriver(clientEngine, true);
        SSLDriver serverDriver = getDriver(serverEngine, false);

        expectThrows(SSLException.class, () -> handshake(clientDriver, serverDriver));
        // In JDK11 we need an non-application write
        if (serverDriver.needsNonApplicationWrite()) {
            serverDriver.nonApplicationWrite();
        }
        // Prior to JDK11 we still need to send a close alert
        if (serverDriver.isClosed() == false) {
            List<String> messages = Arrays.asList("Received fatal alert: handshake_failure",
                "Received close_notify during handshake");
            failedCloseAlert(serverDriver, clientDriver, messages);
        }
    }

    public void testCloseDuringHandshakeJDK11() throws Exception {
        assumeTrue("this tests ssl engine for JDK11", JavaVersion.current().compareTo(JavaVersion.parse("11")) >= 0);
        SSLContext sslContext = getSSLContext();
        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        clientDriver.init();
        serverDriver.init();

        assertTrue(clientDriver.needsNonApplicationWrite());
        assertFalse(serverDriver.needsNonApplicationWrite());
        sendHandshakeMessages(clientDriver, serverDriver);
        sendHandshakeMessages(serverDriver, clientDriver);

        sendData(clientDriver, serverDriver);

        assertTrue(clientDriver.isHandshaking());
        assertTrue(serverDriver.isHandshaking());

        assertFalse(serverDriver.needsNonApplicationWrite());
        serverDriver.initiateClose();
        assertTrue(serverDriver.needsNonApplicationWrite());
        assertFalse(serverDriver.isClosed());
        sendNonApplicationWrites(serverDriver, clientDriver);
        // We are immediately fully closed due to SSLEngine inconsistency
        assertTrue(serverDriver.isClosed());
        // This should not throw exception yet as the SSLEngine will not UNWRAP data while attempting to WRAP
        clientDriver.read(clientBuffer);
        sendNonApplicationWrites(clientDriver, serverDriver);
        clientDriver.read(clientBuffer);
        sendNonApplicationWrites(clientDriver, serverDriver);
        serverDriver.read(serverBuffer);
        assertTrue(clientDriver.isClosed());
    }

    public void testCloseDuringHandshakePreJDK11() throws Exception {
        assumeTrue("this tests ssl engine for pre-JDK11", JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0);
        SSLContext sslContext = getSSLContext();
        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        clientDriver.init();
        serverDriver.init();

        assertTrue(clientDriver.needsNonApplicationWrite());
        assertFalse(serverDriver.needsNonApplicationWrite());
        sendHandshakeMessages(clientDriver, serverDriver);
        sendHandshakeMessages(serverDriver, clientDriver);

        sendData(clientDriver, serverDriver);

        assertTrue(clientDriver.isHandshaking());
        assertTrue(serverDriver.isHandshaking());

        assertFalse(serverDriver.needsNonApplicationWrite());
        serverDriver.initiateClose();
        assertTrue(serverDriver.needsNonApplicationWrite());
        assertFalse(serverDriver.isClosed());
        sendNonApplicationWrites(serverDriver, clientDriver);
        // We are immediately fully closed due to SSLEngine inconsistency
        assertTrue(serverDriver.isClosed());
        // This should not throw exception yet as the SSLEngine will not UNWRAP data while attempting to WRAP
        clientDriver.read(clientBuffer);
        sendNonApplicationWrites(clientDriver, serverDriver);
        SSLException sslException = expectThrows(SSLException.class, () -> clientDriver.read(clientBuffer));
        assertEquals("Received close_notify during handshake", sslException.getMessage());
        assertTrue(clientDriver.needsNonApplicationWrite());
        sendNonApplicationWrites(clientDriver, serverDriver);
        serverDriver.read(serverBuffer);
        assertTrue(clientDriver.isClosed());
    }

    private void failedCloseAlert(SSLDriver sendDriver, SSLDriver receiveDriver, List<String> messages) throws SSLException {
        assertTrue(sendDriver.needsNonApplicationWrite());
        assertFalse(sendDriver.isClosed());

        sendNonApplicationWrites(sendDriver, receiveDriver);
        assertTrue(sendDriver.isClosed());
        sendDriver.close();

        SSLException sslException = expectThrows(SSLException.class, () -> receiveDriver.read(genericBuffer));
        assertTrue("Expected one of the following exception messages: " + messages + ". Found: " + sslException.getMessage(),
            messages.stream().anyMatch(m -> sslException.getMessage().equals(m)));
        if (receiveDriver.needsNonApplicationWrite() == false) {
            assertTrue(receiveDriver.isClosed());
            receiveDriver.close();
        } else {
            assertFalse(receiveDriver.isClosed());
            expectThrows(SSLException.class, receiveDriver::close);
        }
    }

    private SSLContext getSSLContext() throws Exception {
        String certPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt";
        String keyPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem";
        SSLContext sslContext;
        TrustManager tm = CertParsingUtils.trustManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
            (certPath))));
        KeyManager km = CertParsingUtils.keyManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
            (certPath))), PemUtils.readPrivateKey(getDataPath(keyPath), "testclient"::toCharArray), "testclient".toCharArray());
        sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(new KeyManager[] { km }, new TrustManager[] { tm }, new SecureRandom());
        return sslContext;
    }

    private void normalClose(SSLDriver sendDriver, SSLDriver receiveDriver) throws IOException {
        sendDriver.initiateClose();
        assertFalse(sendDriver.readyForApplicationWrites());
        assertTrue(sendDriver.needsNonApplicationWrite());
        sendNonApplicationWrites(sendDriver, receiveDriver);
        assertFalse(sendDriver.isClosed());

        receiveDriver.read(genericBuffer);
        assertFalse(receiveDriver.isClosed());

        assertFalse(receiveDriver.readyForApplicationWrites());
        assertTrue(receiveDriver.needsNonApplicationWrite());
        sendNonApplicationWrites(receiveDriver, sendDriver);
        assertTrue(receiveDriver.isClosed());

        sendDriver.read(genericBuffer);
        assertTrue(sendDriver.isClosed());

        sendDriver.close();
        receiveDriver.close();
    }

    private void sendNonApplicationWrites(SSLDriver sendDriver, SSLDriver receiveDriver) throws SSLException {
        while (sendDriver.needsNonApplicationWrite() || sendDriver.hasFlushPending()) {
            if (sendDriver.hasFlushPending() == false) {
                sendDriver.nonApplicationWrite();
            }
            if (sendDriver.hasFlushPending()) {
                sendData(sendDriver, receiveDriver, true);
            }
        }
    }

    private void handshake(SSLDriver clientDriver, SSLDriver serverDriver) throws IOException {
        handshake(clientDriver, serverDriver, false);
    }

    private void handshake(SSLDriver clientDriver, SSLDriver serverDriver, boolean isRenegotiation) throws IOException {
        if (isRenegotiation == false) {
            clientDriver.init();
            serverDriver.init();
        }

        assertTrue(clientDriver.needsNonApplicationWrite() || clientDriver.hasFlushPending());
        assertFalse(serverDriver.needsNonApplicationWrite());
        sendHandshakeMessages(clientDriver, serverDriver);

        assertTrue(clientDriver.isHandshaking());
        assertTrue(serverDriver.isHandshaking());

        sendHandshakeMessages(serverDriver, clientDriver);

        assertTrue(clientDriver.isHandshaking());
        assertTrue(serverDriver.isHandshaking());

        sendHandshakeMessages(clientDriver, serverDriver);

        assertTrue(clientDriver.isHandshaking());
        assertTrue(serverDriver.isHandshaking());

        sendHandshakeMessages(serverDriver, clientDriver);

        assertFalse(clientDriver.isHandshaking());
        assertFalse(serverDriver.isHandshaking());
    }

    private void sendHandshakeMessages(SSLDriver sendDriver, SSLDriver receiveDriver) throws IOException {
        assertTrue(sendDriver.needsNonApplicationWrite() || sendDriver.hasFlushPending());

        while (sendDriver.needsNonApplicationWrite() || sendDriver.hasFlushPending()) {
            if (sendDriver.hasFlushPending() == false) {
                sendDriver.nonApplicationWrite();
            }
            if (sendDriver.isHandshaking()) {
                assertTrue(sendDriver.hasFlushPending());
                sendData(sendDriver, receiveDriver);
                assertFalse(sendDriver.hasFlushPending());
                receiveDriver.read(genericBuffer);
            }
        }
        if (receiveDriver.isHandshaking()) {
            assertTrue(receiveDriver.needsNonApplicationWrite() || receiveDriver.hasFlushPending());
        }
    }

    private void sendAppData(SSLDriver sendDriver, SSLDriver receiveDriver, ByteBuffer[] message) throws IOException {

        assertFalse(sendDriver.needsNonApplicationWrite());

        int bytesToEncrypt = Arrays.stream(message).mapToInt(Buffer::remaining).sum();

        int bytesEncrypted = 0;
        while (bytesToEncrypt > bytesEncrypted) {
            bytesEncrypted += sendDriver.applicationWrite(message);
            sendData(sendDriver, receiveDriver);
        }
    }

    private void sendData(SSLDriver sendDriver, SSLDriver receiveDriver) {
        sendData(sendDriver, receiveDriver, randomBoolean());
    }

    private void sendData(SSLDriver sendDriver, SSLDriver receiveDriver, boolean partial) {
        ByteBuffer writeBuffer = sendDriver.getNetworkWriteBuffer();
        ByteBuffer readBuffer = receiveDriver.getNetworkReadBuffer();
        if (partial) {
            int initialLimit = writeBuffer.limit();
            int bytesToWrite = writeBuffer.remaining() / (randomInt(2) + 2);
            writeBuffer.limit(writeBuffer.position() + bytesToWrite);
            readBuffer.put(writeBuffer);
            writeBuffer.limit(initialLimit);
            assertTrue(sendDriver.hasFlushPending());
            readBuffer.put(writeBuffer);
            assertFalse(sendDriver.hasFlushPending());

        } else {
            readBuffer.put(writeBuffer);
            assertFalse(sendDriver.hasFlushPending());
        }
    }

    private SSLDriver getDriver(SSLEngine engine, boolean isClient) {
        return new SSLDriver(engine, isClient);
    }
}
