/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email.support;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.subethamail.smtp.auth.EasyAuthenticationHandlerFactory;
import org.subethamail.smtp.helper.SimpleMessageListener;
import org.subethamail.smtp.helper.SimpleMessageListenerAdapter;
import org.subethamail.smtp.server.SMTPServer;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * An mini email smtp server that can be used for unit testing
 */
public class EmailServer {

    public static final String USERNAME = "_user";
    public static final String PASSWORD = "_passwd";

    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private final SMTPServer server;

    public EmailServer(String host, @Nullable SSLContext sslContext, final Logger logger) {
        final SimpleMessageListenerAdapter listener = new SimpleMessageListenerAdapter(new SimpleMessageListener() {
            @Override
            public boolean accept(String from, String recipient) {
                return true;
            }

            @Override
            public void deliver(String from, String recipient, InputStream data) throws IOException {
                try {
                    Session session = Session.getInstance(new Properties());
                    MimeMessage msg = new MimeMessage(session, data);
                    for (Listener listener1 : listeners) {
                        try {
                            listener1.on(msg);
                        } catch (Exception e) {
                            logger.error("Unexpected failure", e);
                            fail(e.getMessage());
                        }
                    }
                } catch (MessagingException me) {
                    throw new RuntimeException("could not create mime message", me);
                }
            }
        });
        final EasyAuthenticationHandlerFactory authentication = new EasyAuthenticationHandlerFactory((user, passwd) -> {
            assertThat(user, is(USERNAME));
            assertThat(passwd, is(PASSWORD));
        });
        server = new SMTPServer(listener, authentication) {
            @Override
            public SSLSocket createSSLSocket(Socket socket) throws IOException {
                if (sslContext == null) {
                    return super.createSSLSocket(socket);
                } else {
                    SSLSocketFactory factory = sslContext.getSocketFactory();
                    InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
                    SSLSocket sslSocket = (SSLSocket) factory.createSocket(socket, remoteAddress.getHostString(), socket.getPort(), true);
                    sslSocket.setUseClientMode(false);
                    sslSocket.setEnabledCipherSuites(sslSocket.getSupportedCipherSuites());
                    return sslSocket;
                }
            }
        };
        server.setHostName(host);
        server.setPort(0);
        if (sslContext != null) {
            server.setEnableTLS(true);
            server.setRequireTLS(true);
            server.setHideTLS(false);
        }
    }

    /**
     * @return the port that the underlying server is listening on
     */
    public int port() {
        return server.getPort();
    }

    public void start() {
        // Must have privileged access because underlying server will accept socket connections
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            server.start();
            return null;
        });
    }

    public void stop() {
        server.stop();
        listeners.clear();
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void clearListeners() {
        this.listeners.clear();
    }

    public static EmailServer localhost(final Logger logger) {
        return localhost(logger, null);
    }

    public static EmailServer localhost(final Logger logger, @Nullable SSLContext sslContext) {
        EmailServer server = new EmailServer("localhost", sslContext, logger);
        server.start();
        return server;
    }

    @FunctionalInterface
    public interface Listener {
        void on(MimeMessage message) throws Exception;
    }
}
