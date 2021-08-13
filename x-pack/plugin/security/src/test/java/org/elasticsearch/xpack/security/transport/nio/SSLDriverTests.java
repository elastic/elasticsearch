/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;

public class SSLDriverTests extends ESTestCase {

    private final IntFunction<Page> pageAllocator = (n) -> new Page(ByteBuffer.allocate(n), () -> {});

    private final InboundChannelBuffer networkReadBuffer = new InboundChannelBuffer(pageAllocator);
    private final InboundChannelBuffer applicationBuffer = new InboundChannelBuffer(pageAllocator);
    private final AtomicInteger openPages = new AtomicInteger(0);

    public void testPingPongAndClose() throws Exception {
        assumeFalse("Fails in normalClose as receiveDriver.getOutboundBuffer().hasEncryptedBytesToFlush() is false", inFipsJvm());
        SSLContext sslContext = getSSLContext();

        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        handshake(clientDriver, serverDriver);

        ByteBuffer[] buffers = {ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))};
        sendAppData(clientDriver, buffers);
        serverDriver.read(networkReadBuffer, applicationBuffer);
        assertEquals(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8)), applicationBuffer.sliceBuffersTo(4)[0]);
        applicationBuffer.release(4);

        ByteBuffer[] buffers2 = {ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8))};
        sendAppData(serverDriver, buffers2);
        clientDriver.read(networkReadBuffer, applicationBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), applicationBuffer.sliceBuffersTo(4)[0]);
        applicationBuffer.release(4);

        normalClose(clientDriver, serverDriver);
    }

    public void testDataStoredInOutboundBufferIsClosed() throws Exception {
        SSLContext sslContext = getSSLContext();

        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        handshake(clientDriver, serverDriver);

        ByteBuffer[] buffers = {ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))};
        serverDriver.write(new FlushOperation(buffers, (v, e) -> {}));

        expectThrows(SSLException.class, serverDriver::close);
        assertEquals(0, openPages.get());
    }

    public void testRenegotiate() throws Exception {
        assumeFalse("BCTLS doesn't support renegotiation: https://github.com/bcgit/bc-java/issues/593#issuecomment-533518845",
            inFipsJvm());
        SSLContext sslContext = getSSLContext();

        SSLEngine serverEngine = sslContext.createSSLEngine();
        SSLEngine clientEngine = sslContext.createSSLEngine();

        // Lock the protocol to 1.2 as 1.3 does not support renegotiation
        String[] serverProtocols = {"TLSv1.2"};
        serverEngine.setEnabledProtocols(serverProtocols);
        String[] clientProtocols = {"TLSv1.2"};
        clientEngine.setEnabledProtocols(clientProtocols);
        SSLDriver clientDriver = getDriver(clientEngine, true);
        SSLDriver serverDriver = getDriver(serverEngine, false);

        handshake(clientDriver, serverDriver);

        ByteBuffer[] buffers = {ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8))};
        sendAppData(clientDriver, buffers);
        serverDriver.read(networkReadBuffer, applicationBuffer);
        assertEquals(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8)), applicationBuffer.sliceBuffersTo(4)[0]);
        applicationBuffer.release(4);

        clientDriver.renegotiate();
        assertFalse(clientDriver.readyForApplicationData());

        // This tests that the client driver can still receive data based on the prior handshake
        ByteBuffer[] buffers2 = {ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8))};
        sendAppData(serverDriver, buffers2);
        clientDriver.read(networkReadBuffer, applicationBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), applicationBuffer.sliceBuffersTo(4)[0]);
        applicationBuffer.release(4);

        handshake(clientDriver, serverDriver, true);
        sendAppData(clientDriver, buffers);
        serverDriver.read(networkReadBuffer, applicationBuffer);
        assertEquals(ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8)), applicationBuffer.sliceBuffersTo(4)[0]);
        applicationBuffer.release(4);
        sendAppData(serverDriver, buffers2);
        clientDriver.read(networkReadBuffer, applicationBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), applicationBuffer.sliceBuffersTo(4)[0]);
        applicationBuffer.release(4);

        normalClose(clientDriver, serverDriver);
    }

    public void testBigApplicationData() throws Exception {
        assumeFalse("Fails in normalClose as receiveDriver.getOutboundBuffer().hasEncryptedBytesToFlush() is false", inFipsJvm());
        SSLContext sslContext = getSSLContext();

        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        handshake(clientDriver, serverDriver);

        ByteBuffer buffer = ByteBuffer.allocate(1 << 15);
        for (int i = 0; i < (1 << 15); ++i) {
            buffer.put((byte) (i % 127));
        }
        buffer.flip();
        ByteBuffer[] buffers = {buffer};
        sendAppData(clientDriver, buffers);
        serverDriver.read(networkReadBuffer, applicationBuffer);
        ByteBuffer[] buffers1 = applicationBuffer.sliceBuffersFrom(0);
        assertEquals((byte) (16383 % 127), buffers1[0].get(16383));
        assertEquals((byte) (32767 % 127), buffers1[1].get(16383));
        applicationBuffer.release(1 << 15);

        ByteBuffer[] buffers2 = {ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8))};
        sendAppData(serverDriver, buffers2);
        clientDriver.read(networkReadBuffer, applicationBuffer);
        assertEquals(ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8)), applicationBuffer.sliceBuffersTo(4)[0]);
        applicationBuffer.release(4);

        normalClose(clientDriver, serverDriver);
    }

    public void testHandshakeFailureBecauseProtocolMismatch() throws Exception {
        SSLContext sslContext = getSSLContext();
        SSLEngine clientEngine = sslContext.createSSLEngine();
        SSLEngine serverEngine = sslContext.createSSLEngine();

        final String[] serverProtocols;
        final String[] clientProtocols;
        final Matcher<String> expectedMessageMatcher;

        if (inFipsJvm()) {
            // fips JSSE does not support TLSv1.3 yet
            serverProtocols = new String[]{"TLSv1.2"};
            clientProtocols = new String[]{"TLSv1.1"};
            expectedMessageMatcher = is("org.bouncycastle.tls.TlsFatalAlert: protocol_version(70)");
        } else if (JavaVersion.current().compareTo(JavaVersion.parse("16")) >= 0) {
            // JDK16 https://jdk.java.net/16/release-notes does not permit protocol TLSv1.1 OOB
            serverProtocols = new String[]{"TLSv1.3"};
            clientProtocols = new String[]{"TLSv1.2"};
            expectedMessageMatcher = is("The client supported protocol versions [TLSv1.2] are not accepted by server preferences [TLS13]");
        } else {
            serverProtocols = new String[]{"TLSv1.2"};
            clientProtocols = new String[]{"TLSv1.1"};
            expectedMessageMatcher = anyOf(
                is("No appropriate protocol (protocol is disabled or cipher suites are inappropriate)"),
                is("The client supported protocol versions [TLSv1.1] are not accepted by server preferences [TLS12]"));
        }

        serverEngine.setEnabledProtocols(serverProtocols);
        clientEngine.setEnabledProtocols(clientProtocols);
        SSLDriver clientDriver = getDriver(clientEngine, true);
        SSLDriver serverDriver = getDriver(serverEngine, false);

        SSLException sslException = expectThrows(SSLException.class, () -> handshake(clientDriver, serverDriver));
        assertThat(sslException.getMessage(), expectedMessageMatcher);

        // Prior to JDK11 we still need to send a close alert
        if (serverDriver.isClosed() == false) {
            if (false == inFipsJvm() && false == serverDriver.getOutboundBuffer().hasEncryptedBytesToFlush()) {
                serverDriver.getSSLEngine().closeInbound();
                serverDriver.getSSLEngine().closeOutbound();
                serverDriver.close();;
                assertTrue(serverDriver.isClosed());
                clientDriver.close();
                assertTrue(clientDriver.isClosed());
            } else {
                failedCloseAlert(serverDriver, clientDriver, Arrays.asList("Received fatal alert: protocol_version",
                    "Received fatal alert: handshake_failure"));
            }
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

        // Prior to JDK11 we still need to send a close alert
        if (serverDriver.isClosed() == false) {
            List<String> messages = Arrays.asList("Received fatal alert: handshake_failure",
                "Received close_notify during handshake");
            failedCloseAlert(serverDriver, clientDriver, messages);
        }
    }

    public void testCloseDuringHandshakeJDK11() throws Exception {
        assumeTrue("this tests ssl engine for JDK11",
            JavaVersion.current().compareTo(JavaVersion.parse("11")) >= 0 && inFipsJvm() == false);
        SSLContext sslContext = getSSLContext();
        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        clientDriver.init();
        serverDriver.init();

        assertTrue(clientDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        sendHandshakeMessages(clientDriver, serverDriver);

        // Sometimes send server messages before closing
        if (randomBoolean()) {
            sendHandshakeMessages(serverDriver, clientDriver);

            if ("TLSv1.3".equals(clientDriver.getSSLEngine().getEnabledProtocols()[0])) {
                assertTrue(clientDriver.readyForApplicationData());
            } else {
                assertFalse(clientDriver.readyForApplicationData());
            }
        }

        assertFalse(serverDriver.readyForApplicationData());

        serverDriver.initiateClose();
        assertTrue(serverDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        // We are immediately fully closed due to SSLEngine inconsistency
        assertTrue(serverDriver.isClosed());

        sendNonApplicationWrites(serverDriver);
        clientDriver.read(networkReadBuffer, applicationBuffer);
        assertTrue(clientDriver.isClosed());

        sendNonApplicationWrites(clientDriver);
        serverDriver.read(networkReadBuffer, applicationBuffer);
    }

    public void testCloseDuringHandshakePreJDK11() throws Exception {
        assumeTrue("this tests ssl engine for pre-JDK11", JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0);
        SSLContext sslContext = getSSLContext();
        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        clientDriver.init();
        serverDriver.init();

        assertTrue(clientDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        sendHandshakeMessages(clientDriver, serverDriver);
        sendHandshakeMessages(serverDriver, clientDriver);

        assertFalse(clientDriver.readyForApplicationData());
        assertFalse(serverDriver.readyForApplicationData());

        serverDriver.initiateClose();
        assertTrue(serverDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        assertFalse(serverDriver.isClosed());
        sendNonApplicationWrites(serverDriver);
        // We are immediately fully closed due to SSLEngine inconsistency
        assertTrue(serverDriver.isClosed());
        // This should not throw exception yet as the SSLEngine will not UNWRAP data while attempting to WRAP
        clientDriver.read(networkReadBuffer, applicationBuffer);
        sendNonApplicationWrites(clientDriver);
        SSLException sslException = expectThrows(SSLException.class, () -> clientDriver.read(networkReadBuffer, applicationBuffer));
        assertEquals("Received close_notify during handshake", sslException.getMessage());
        assertTrue(clientDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        sendNonApplicationWrites(clientDriver);
        serverDriver.read(networkReadBuffer, applicationBuffer);
        assertTrue(clientDriver.isClosed());
    }

    private void failedCloseAlert(SSLDriver sendDriver, SSLDriver receiveDriver, List<String> messages) throws SSLException {
        assertTrue(sendDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        assertFalse(sendDriver.isClosed());

        sendNonApplicationWrites(sendDriver);
        assertTrue(sendDriver.isClosed());
        sendDriver.close();

        SSLException sslException = expectThrows(SSLException.class, () -> receiveDriver.read(networkReadBuffer, applicationBuffer));
        assertTrue("Expected one of the following exception messages: " + messages + ". Found: " + sslException.getMessage(),
            messages.stream().anyMatch(m -> sslException.getMessage().equals(m)));
        assertTrue(receiveDriver.isClosed());
        receiveDriver.close();
    }

    private SSLContext getSSLContext() throws Exception {
        String certPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt";
        String keyPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem";
        SSLContext sslContext;
        TrustManager tm = CertParsingUtils.trustManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
            (certPath))));
        KeyManager km = CertParsingUtils.keyManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
            (certPath))), PemUtils.readPrivateKey(getDataPath(keyPath), "testclient"::toCharArray), "testclient".toCharArray());
        sslContext = SSLContext.getInstance(inFipsJvm() ? "TLSv1.2" : randomFrom("TLSv1.2", "TLSv1.3"));
        sslContext.init(new KeyManager[] { km }, new TrustManager[] { tm }, new SecureRandom());
        return sslContext;
    }

    private void normalClose(SSLDriver sendDriver, SSLDriver receiveDriver) throws IOException {
        sendDriver.initiateClose();
        assertFalse(sendDriver.readyForApplicationData());
        assertTrue(sendDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        sendNonApplicationWrites(sendDriver);
        assertFalse(sendDriver.isClosed());

        receiveDriver.read(networkReadBuffer, applicationBuffer);
        assertTrue(receiveDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        assertTrue(receiveDriver.isClosed());

        sendNonApplicationWrites(receiveDriver);

        sendDriver.read(networkReadBuffer, applicationBuffer);
        assertTrue(sendDriver.isClosed());

        sendDriver.close();
        receiveDriver.close();
        assertEquals(0, openPages.get());
    }

    private void sendNonApplicationWrites(SSLDriver sendDriver) {
        SSLOutboundBuffer outboundBuffer = sendDriver.getOutboundBuffer();
        sendData(outboundBuffer.buildNetworkFlushOperation());
    }

    private void handshake(SSLDriver clientDriver, SSLDriver serverDriver) throws IOException {
        handshake(clientDriver, serverDriver, false);
    }

    private void handshake(SSLDriver clientDriver, SSLDriver serverDriver, boolean isRenegotiation) throws IOException {
        if (isRenegotiation == false) {
            clientDriver.init();
            serverDriver.init();
        }

        assertTrue(clientDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        sendHandshakeMessages(clientDriver, serverDriver);

        assertFalse(clientDriver.readyForApplicationData());
        assertFalse(serverDriver.readyForApplicationData());

        sendHandshakeMessages(serverDriver, clientDriver);

        if ("TLSv1.3".equals(clientDriver.getSSLEngine().getEnabledProtocols()[0])) {
            assertTrue(clientDriver.readyForApplicationData());
            assertFalse(serverDriver.readyForApplicationData());

            sendHandshakeMessages(clientDriver, serverDriver);

            assertTrue(clientDriver.readyForApplicationData());
            assertTrue(serverDriver.readyForApplicationData());
        } else {
            assertFalse(clientDriver.readyForApplicationData());
            assertFalse(serverDriver.readyForApplicationData());

            sendHandshakeMessages(clientDriver, serverDriver);

            assertFalse(clientDriver.readyForApplicationData());
            assertTrue(serverDriver.readyForApplicationData());

            sendHandshakeMessages(serverDriver, clientDriver);

            assertTrue(clientDriver.readyForApplicationData());
            assertTrue(serverDriver.readyForApplicationData());
        }


    }

    private void sendHandshakeMessages(SSLDriver sendDriver, SSLDriver receiveDriver) throws IOException {
        assertTrue(sendDriver.getOutboundBuffer().hasEncryptedBytesToFlush());

        sendData(sendDriver.getOutboundBuffer().buildNetworkFlushOperation());
        receiveDriver.read(networkReadBuffer, applicationBuffer);
        if (receiveDriver.readyForApplicationData() == false) {
            assertTrue(receiveDriver.getOutboundBuffer().hasEncryptedBytesToFlush());
        }
    }

    private void sendAppData(SSLDriver sendDriver, ByteBuffer[] message) throws IOException {
        FlushOperation flushOperation = new FlushOperation(message, (r, l) -> {});

        while (flushOperation.isFullyFlushed() == false) {
            sendDriver.write(flushOperation);
        }
        sendData(sendDriver.getOutboundBuffer().buildNetworkFlushOperation());
    }

    private void sendData(FlushOperation flushOperation) {
        ByteBuffer[] writeBuffers = flushOperation.getBuffersToWrite();
        int bytesToCopy = Arrays.stream(writeBuffers).mapToInt(Buffer::remaining).sum();
        networkReadBuffer.ensureCapacity(bytesToCopy + networkReadBuffer.getIndex());
        ByteBuffer[] byteBuffers = networkReadBuffer.sliceBuffersFrom(0);
        assert  writeBuffers.length > 0 : "No write buffers";

        int r = 0;
        while (flushOperation.isFullyFlushed() == false) {
            ByteBuffer readBuffer = byteBuffers[r];
            ByteBuffer writeBuffer = flushOperation.getBuffersToWrite()[0];
            int toWrite = Math.min(writeBuffer.remaining(), readBuffer.remaining());
            writeBuffer.limit(writeBuffer.position() + toWrite);
            readBuffer.put(writeBuffer);
            flushOperation.incrementIndex(toWrite);
            if (readBuffer.remaining() == 0) {
                r++;
            }
        }
        networkReadBuffer.incrementIndex(bytesToCopy);

        assertTrue(flushOperation.isFullyFlushed());
        flushOperation.getListener().accept(null, null);
    }

    private SSLDriver getDriver(SSLEngine engine, boolean isClient) {
        return new SSLDriver(engine, (n) -> {
            openPages.incrementAndGet();
            return new Page(ByteBuffer.allocate(n), openPages::decrementAndGet);
        }, isClient);
    }
}
