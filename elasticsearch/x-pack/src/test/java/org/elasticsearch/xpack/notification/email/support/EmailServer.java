/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.transport.PortsRange;
import org.subethamail.smtp.TooMuchDataException;
import org.subethamail.smtp.auth.EasyAuthenticationHandlerFactory;
import org.subethamail.smtp.auth.LoginFailedException;
import org.subethamail.smtp.auth.UsernamePasswordValidator;
import org.subethamail.smtp.helper.SimpleMessageListener;
import org.subethamail.smtp.helper.SimpleMessageListenerAdapter;
import org.subethamail.smtp.server.SMTPServer;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * An mini email smtp server that can be used for unit testing
 *
 *
 */
public class EmailServer {

    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    private final SMTPServer server;

    public EmailServer(String host, int port, final String username, final String password, final ESLogger logger) {
        server = new SMTPServer(new SimpleMessageListenerAdapter(new SimpleMessageListener() {
            @Override
            public boolean accept(String from, String recipient) {
                return true;
            }

            @Override
            public void deliver(String from, String recipient, InputStream data) throws TooMuchDataException, IOException {
                try {
                    Session session = Session.getInstance(new Properties());
                    MimeMessage msg = new MimeMessage(session, data);
                    for (Listener listener : listeners) {
                        try {
                            listener.on(msg);
                        } catch (Exception e) {
                            logger.error("Unexpected failure", e);
                            fail(e.getMessage());
                        }
                    }
                } catch (MessagingException me) {
                    throw new RuntimeException("could not create mime message", me);
                }
            }
        }), new EasyAuthenticationHandlerFactory(new UsernamePasswordValidator() {
            @Override
            public void login(String user, String passwd) throws LoginFailedException {
                assertThat(user, is(username));
                assertThat(passwd, is(password));
            }
        }));
        server.setHostName(host);
        server.setPort(port);
    }

    /**
     * @return the port that the underlying server is listening on
     */
    public int port() {
        return server.getPort();
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop();
        listeners.clear();
    }

    public Listener.Handle addListener(Listener listener) {
        listeners.add(listener);
        return new Listener.Handle(listeners, listener);
    }

    public static EmailServer localhost(String portRangeStr, final String username, final String password, final ESLogger logger) {
        final AtomicReference<EmailServer> emailServer = new AtomicReference<>();
        boolean bound = new PortsRange(portRangeStr).iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int port) {
                try {
                    EmailServer server = new EmailServer("localhost", port, username, password, logger);
                    server.start();
                    emailServer.set(server);
                    return true;
                } catch (RuntimeException re) {
                    if (re.getCause() instanceof BindException) {
                        logger.warn("port [{}] was already in use trying next port", re, port);
                        return false;
                    } else {
                        throw re;
                    }
                }
            }
        });
        if (!bound || emailServer.get() == null) {
            throw new ElasticsearchException("could not bind to any of the port in [" + portRangeStr + "]");
        }
        return emailServer.get();
    }



    public interface Listener {

        void on(MimeMessage message) throws Exception;

        class Handle {

            private final List<Listener> listeners;
            private final Listener listener;

            Handle(List<Listener> listeners, Listener listener) {
                this.listeners = listeners;
                this.listener = listener;
            }

            public void remove() {
                listeners.remove(listener);
            }
        }

    }
}
