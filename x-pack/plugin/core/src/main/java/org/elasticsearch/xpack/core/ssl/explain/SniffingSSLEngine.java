/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import org.elasticsearch.common.Nullable;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This SSL engine wraps another SSL engine to which it delegates all SSL/TLS processing.
 * It also captures some key information passing through that TLS stream that is not otherwise available in the JSSE API.
 */
public class SniffingSSLEngine extends SSLEngine {
    private final SSLEngine engine;

    /**
     * The cipher suites that were provided in the {@link TlsHandshake.ClientHello} message, or null if the ClientHello has not yet been
     * seen in this TLS exchange.
     */
    private TlsCipherSuites clientCipherSuites;

    /**
     * Bytes that have past through this stream but have not yet been processed (because they do not compose a complete TLS message)
     */
    private ByteBuffer inputBuffer;

    public SniffingSSLEngine(SSLEngine engine) {
        this.engine = engine;
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) throws SSLException {
        return engine.wrap(srcs, offset, length, dst);
    }

    /**
     * @return {@code true} if this engine should inspect incoming TLS messages,
     * {@code false} if we have captured all information we are interested in.
     */
    private boolean shouldSniff() {
        return clientCipherSuites == null && engine.getUseClientMode() == false;
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
        if (shouldSniff()) {
            inspectBuffer(src);
        }
        return engine.unwrap(src, dst);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts) throws SSLException {
        if (shouldSniff()) {
            inspectBuffer(src);
        }
        return engine.unwrap(src, dsts);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) throws SSLException {
        if (shouldSniff()) {
            inspectBuffer(src);
        }
        return engine.unwrap(src, dsts, offset, length);
    }

    /**
     * Inspects the incoming buffer and processes any relevant messages
     */
    private void inspectBuffer(ByteBuffer src) {
        // Include any existing buffered input
        if (inputBuffer != null) {
            final ByteBuffer newBuffer = ByteBuffer.allocate(inputBuffer.remaining() + src.remaining());
            newBuffer.put(inputBuffer);
            src.mark();
            newBuffer.put(src);
            src.reset();
            src = newBuffer;
        } else {
            src = src.asReadOnlyBuffer();
        }
        while (src.hasRemaining()) {
            final TlsPlaintext plaintext = TlsPlaintext.parse(src);
            if (plaintext == null) {
                inputBuffer = src.asReadOnlyBuffer();
                return;
            }
            processInput(plaintext);
        }
    }

    private void processInput(TlsPlaintext plaintext) {
        switch (plaintext.getContentType()) {
            case Handshake:
                processHandshake(plaintext);
                return;
            case Alert:
            case ChangeCipherSpec:
            case ApplicationData:
            default:
                // skip
                return;
        }
    }

    private void processHandshake(TlsPlaintext plaintext) {
        final TlsHandshake handshake = TlsHandshake.parse(plaintext.getPayload());
        if (handshake != null && handshake.type() == TlsHandshakeType.ClientHello) {
            this.clientCipherSuites = ((TlsHandshake.ClientHello) handshake).cipherSuites();
        }

    }

    @Override
    public Runnable getDelegatedTask() {
        return engine.getDelegatedTask();
    }

    @Override
    public void closeInbound() throws SSLException {
        engine.closeInbound();
    }

    @Override
    public boolean isInboundDone() {
        return engine.isInboundDone();
    }

    @Override
    public void closeOutbound() {
        engine.closeOutbound();
    }

    @Override
    public boolean isOutboundDone() {
        return engine.isOutboundDone();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return engine.getSupportedCipherSuites();
    }

    @Override
    public String[] getEnabledCipherSuites() {
        return engine.getEnabledCipherSuites();
    }

    @Override
    public void setEnabledCipherSuites(String[] suites) {
        engine.setEnabledCipherSuites(suites);
    }

    @Override
    public String[] getSupportedProtocols() {
        return engine.getSupportedProtocols();
    }

    @Override
    public String[] getEnabledProtocols() {
        return engine.getEnabledProtocols();
    }

    @Override
    public void setEnabledProtocols(String[] protocols) {
        engine.setEnabledProtocols(protocols);
    }

    @Override
    public SSLSession getSession() {
        return engine.getSession();
    }

    @Override
    public void beginHandshake() throws SSLException {
        engine.beginHandshake();
    }

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
        return engine.getHandshakeStatus();
    }

    @Override
    public void setUseClientMode(boolean mode) {
        engine.setUseClientMode(mode);
    }

    @Override
    public boolean getUseClientMode() {
        return engine.getUseClientMode();
    }

    @Override
    public void setNeedClientAuth(boolean need) {
        engine.setNeedClientAuth(need);
    }

    @Override
    public boolean getNeedClientAuth() {
        return engine.getNeedClientAuth();
    }

    @Override
    public void setWantClientAuth(boolean want) {
        engine.setWantClientAuth(want);
    }

    @Override
    public boolean getWantClientAuth() {
        return engine.getWantClientAuth();
    }

    @Override
    public void setEnableSessionCreation(boolean flag) {
        engine.setEnableSessionCreation(flag);
    }

    @Override
    public boolean getEnableSessionCreation() {
        return engine.getEnableSessionCreation();
    }

    @Nullable
    public List<String> getRemoteCipherSuites() {
        if (this.clientCipherSuites != null) {
            return clientCipherSuites.names();
        }
        return null;
    }
}
