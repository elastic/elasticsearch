/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SimpleBindRequest;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.mocksocket.MockSocket;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests that the server sets properly load balance connections without throwing exceptions
 */
public class SessionFactoryLoadBalancingTests extends LdapTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("SessionFactoryLoadBalancingTests thread pool");
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testRoundRobin() throws Exception {
        try (TestSessionFactory testSessionFactory = createSessionFactory(LdapLoadBalancing.ROUND_ROBIN)) {
            final int numberOfIterations = randomIntBetween(1, 5);
            for (int iteration = 0; iteration < numberOfIterations; iteration++) {
                for (int i = 0; i < numberOfLdapServers; i++) {
                    try (LDAPConnection connection = LdapUtils.privilegedConnect(testSessionFactory.getServerSet()::getConnection)) {
                        assertThat(connection.getConnectedPort(), is(ldapServers[i].getListenPort()));
                    }
                }
            }
        }
    }

    public void testRoundRobinWithFailures() throws Exception {
        assumeTrue("at least two ldap servers should be present for this test", ldapServers.length > 1);
        logger.debug("using [{}] ldap servers, urls {}", ldapServers.length, ldapUrls());
        TestSessionFactory testSessionFactory = createSessionFactory(LdapLoadBalancing.ROUND_ROBIN);

        // create a list of ports
        List<Integer> ports = new ArrayList<>(numberOfLdapServers);
        for (InMemoryDirectoryServer ldapServer : ldapServers) {
            ports.add(ldapServer.getListenPort());
        }
        logger.debug("list of all ports {}", ports);

        final int numberToKill = randomIntBetween(1, numberOfLdapServers - 1);
        logger.debug("killing [{}] servers", numberToKill);

        // get a subset to kill
        final List<InMemoryDirectoryServer> ldapServersToKill = randomSubsetOf(numberToKill, ldapServers);
        final List<InMemoryDirectoryServer> ldapServersList = Arrays.asList(ldapServers);
        final MockServerSocket mockServerSocket = new MockServerSocket(0, 0);
        final List<Thread> listenThreads = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(ldapServersToKill.size());
        final CountDownLatch closeLatch = new CountDownLatch(1);
        final Set<Integer> shutdownPorts = new HashSet<>(numberToKill);
        try {
            final AtomicBoolean success = new AtomicBoolean(true);
            for (InMemoryDirectoryServer ldapServerToKill : ldapServersToKill) {
                final int index = ldapServersList.indexOf(ldapServerToKill);
                assertThat(index, greaterThanOrEqualTo(0));
                final int port = ldapServers[index].getListenPort();
                logger.debug("shutting down server index [{}] listening on [{}]", index, port);
                assertTrue(ports.remove(Integer.valueOf(port)));
                shutdownPorts.add(port);
                ldapServers[index].shutDown(true);

                // when running multiple test jvms, there is a chance that something else could
                // start listening on this port so we try to avoid this by creating a local socket
                // that will be bound to the port the ldap server was running on and connecting to
                // a mock server socket.
                // NOTE: this is not perfect as there is a small amount of time between the shutdown
                // of the ldap server and the opening of the socket
                logger.debug("opening mock client sockets bound to [{}]", port);
                Runnable runnable = new PortBlockingRunnable(
                    mockServerSocket.getInetAddress(),
                    mockServerSocket.getLocalPort(),
                    port,
                    latch,
                    closeLatch,
                    success
                );
                Thread thread = new Thread(runnable);
                thread.start();
                listenThreads.add(thread);

                assertThat(ldapServers[index].getListenPort(), is(-1));
            }

            latch.await();

            assumeTrue(
                "Failed to open sockets on all addresses with the port that an LDAP server was bound to. Some operating systems "
                    + "allow binding to an address and port combination even if an application is bound to the port on a wildcard address",
                success.get()
            );
            final int numberOfIterations = randomIntBetween(1, 5);
            logger.debug("list of all open ports {}", ports);
            // go one iteration through and attempt a bind
            final Map<Integer, AtomicInteger> bindCountPerPort = new HashMap<>();
            for (int iteration = 0; iteration < numberOfIterations; iteration++) {
                logger.debug("iteration [{}]", iteration);
                for (Integer expectedPort : ports) {
                    logger.debug("attempting connection with expected port [{}]", expectedPort);
                    LDAPConnection connection = null;
                    try {
                        do {
                            final LDAPConnection finalConnection = LdapUtils.privilegedConnect(
                                testSessionFactory.getServerSet()::getConnection
                            );
                            connection = finalConnection;
                            final int connectedPort = finalConnection.getConnectedPort();
                            logger.debug("established connection with port [{}] expected port [{}]", connectedPort, expectedPort);
                            bindCountPerPort.computeIfAbsent(connectedPort, ignore -> new AtomicInteger(0)).incrementAndGet();
                            if (connectedPort != expectedPort) {
                                if (shutdownPorts.contains(connectedPort)) {
                                    // If we connected to the shutdown port, verify that it's something unbindable (our mock socket)
                                    LDAPException e = expectThrows(
                                        LDAPException.class,
                                        () -> finalConnection.bind(new SimpleBindRequest())
                                    );
                                    assertThat(e.getMessage(), containsString("not connected"));
                                    finalConnection.close();
                                } else if (ports.contains(connectedPort) == false) {
                                    fail(
                                        "Round robin scheme connected to port ["
                                            + connectedPort
                                            + "] but expected either an active port ["
                                            + ports
                                            + "] or one of the shutdown port ["
                                            + shutdownPorts
                                            + "]"
                                    );
                                }
                            }
                        } while (connection.getConnectedPort() != expectedPort);

                        assertThat(connection.getConnectedPort(), is(expectedPort));
                    } finally {
                        if (connection != null) {
                            connection.close();
                        }
                    }
                }
            }
            if (com.unboundid.ldap.sdk.Version.getShortVersionString().equals("unboundid-ldapsdk-6.0.2")) {
                // This is the behaviour in 6.0.2, but per it should be fixed in 6.0.3
                // See: https://github.com/pingidentity/ldapsdk/issues/118
                // See: https://github.com/pingidentity/ldapsdk/commit/2d08c5258c3a62b7c90cd4e878c4a0d25ae01a82
                ports.forEach(port -> {
                    int count = bindCountPerPort.get(port).get();
                    assertThat("Connections to port [" + port + "]", count, greaterThanOrEqualTo(numberOfIterations));
                    assertThat("Connections to port [" + port + "]", count, lessThanOrEqualTo(numberOfIterations * (1 + numberToKill)));
                });
            } else {
                // Best guess about what behaviour to expect in 6.0.3 or later.
                // This may need to be tweaked when we do an upgrade of LDAP SDK
                ports.forEach(port -> {
                    int count = bindCountPerPort.get(port).get();
                    assertThat("Connections to port [" + port + "]", count, equalTo(numberOfIterations));
                });
            }
        } finally {
            closeLatch.countDown();
            mockServerSocket.close();
            for (Thread t : listenThreads) {
                t.join();
            }
            testSessionFactory.close();
        }
    }

    @SuppressForbidden(reason = "Allow opening socket for test")
    private MockSocket openMockSocket(InetAddress remoteAddress, int remotePort, InetAddress localAddress, int localPort)
        throws IOException {
        final MockSocket socket = new MockSocket();
        socket.setReuseAddress(true); // allow binding even if the previous socket is in timed wait state.
        socket.setSoLinger(true, 0); // close immediately as we are not writing anything here.
        socket.bind(new InetSocketAddress(localAddress, localPort));
        SocketAccess.doPrivileged(() -> socket.connect(new InetSocketAddress(remoteAddress, remotePort)));
        return socket;
    }

    public void testFailover() throws Exception {
        assumeTrue("at least two ldap servers should be present for this test", ldapServers.length > 1);
        logger.debug("using [{}] ldap servers, urls {}", ldapServers.length, ldapUrls());
        TestSessionFactory testSessionFactory = createSessionFactory(LdapLoadBalancing.FAILOVER);

        // first test that there is no round robin stuff going on
        final int firstPort = ldapServers[0].getListenPort();
        for (int i = 0; i < numberOfLdapServers; i++) {
            try (LDAPConnection connection = LdapUtils.privilegedConnect(testSessionFactory.getServerSet()::getConnection)) {
                assertThat(connection.getConnectedPort(), is(firstPort));
            }
        }

        // we need at least one good server. Hence the upper bound is number - 2 since we need at least
        // one server to use!
        InMemoryDirectoryServer[] allButFirstServer = Arrays.copyOfRange(ldapServers, 1, ldapServers.length);
        final List<InMemoryDirectoryServer> ldapServersToKill;
        if (ldapServers.length > 2) {
            final int numberToKill = randomIntBetween(1, numberOfLdapServers - 2);
            ldapServersToKill = randomSubsetOf(numberToKill, allButFirstServer);
            ldapServersToKill.add(ldapServers[0]); // always kill the first one
        } else {
            ldapServersToKill = Collections.singletonList(ldapServers[0]);
        }
        final List<InMemoryDirectoryServer> ldapServersList = Arrays.asList(ldapServers);
        final MockServerSocket mockServerSocket = new MockServerSocket(0, 0);
        final List<Thread> listenThreads = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(ldapServersToKill.size());
        final CountDownLatch closeLatch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(true);
        for (InMemoryDirectoryServer ldapServerToKill : ldapServersToKill) {
            final int index = ldapServersList.indexOf(ldapServerToKill);
            final int port = ldapServers[index].getListenPort();
            logger.debug("shutting down server index [{}] listening on [{}]", index, port);
            ldapServers[index].shutDown(true);

            // when running multiple test jvms, there is a chance that something else could
            // start listening on this port so we try to avoid this by creating a local socket
            // that will be bound to the port the ldap server was running on and connecting to
            // a mock server socket.
            // NOTE: this is not perfect as there is a small amount of time between the shutdown
            // of the ldap server and the opening of the socket
            logger.debug("opening mock server socket listening on [{}]", port);
            Runnable runnable = new PortBlockingRunnable(
                mockServerSocket.getInetAddress(),
                mockServerSocket.getLocalPort(),
                port,
                latch,
                closeLatch,
                success
            );
            Thread thread = new Thread(runnable);
            thread.start();
            listenThreads.add(thread);

            assertThat(ldapServers[index].getListenPort(), is(-1));
        }

        try {
            latch.await();

            assumeTrue(
                "Failed to open sockets on all addresses with the port that an LDAP server was bound to. Some operating systems "
                    + "allow binding to an address and port combination even if an application is bound to the port on a wildcard address",
                success.get()
            );
            int firstNonStoppedPort = -1;
            // now we find the first that isn't stopped
            for (int i = 0; i < numberOfLdapServers; i++) {
                if (ldapServers[i].getListenPort() != -1) {
                    firstNonStoppedPort = ldapServers[i].getListenPort();
                    break;
                }
            }
            logger.debug("first non stopped port [{}]", firstNonStoppedPort);
            assertThat(firstNonStoppedPort, not(-1));
            final int numberOfIterations = randomIntBetween(1, 5);
            for (int iteration = 0; iteration < numberOfIterations; iteration++) {
                logger.debug("attempting connection with expected port [{}] iteration [{}]", firstNonStoppedPort, iteration);
                LDAPConnection connection = null;
                try {
                    do {
                        final LDAPConnection finalConnection = LdapUtils.privilegedConnect(
                            testSessionFactory.getServerSet()::getConnection
                        );
                        connection = finalConnection;
                        logger.debug(
                            "established connection with port [{}] expected port [{}]",
                            finalConnection.getConnectedPort(),
                            firstNonStoppedPort
                        );
                        if (finalConnection.getConnectedPort() != firstNonStoppedPort) {
                            LDAPException e = expectThrows(LDAPException.class, () -> finalConnection.bind(new SimpleBindRequest()));
                            assertThat(e.getMessage(), containsString("not connected"));
                            finalConnection.close();
                        }
                    } while (connection.getConnectedPort() != firstNonStoppedPort);

                    assertThat(connection.getConnectedPort(), is(firstNonStoppedPort));
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        } finally {
            closeLatch.countDown();
            mockServerSocket.close();
            for (Thread t : listenThreads) {
                t.join();
            }
            testSessionFactory.close();
        }
    }

    private TestSessionFactory createSessionFactory(LdapLoadBalancing loadBalancing) throws Exception {
        String groupSearchBase = "cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        Settings settings = buildLdapSettings(
            ldapUrls(),
            new String[] { userTemplate },
            groupSearchBase,
            LdapSearchScope.SUB_TREE,
            loadBalancing
        );
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).put(settings).build();
        RealmConfig config = new RealmConfig(
            REALM_IDENTIFIER,
            globalSettings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        return new TestSessionFactory(config, new SSLService(TestEnvironment.newEnvironment(config.settings())), threadPool);
    }

    private class PortBlockingRunnable implements Runnable {

        private final InetAddress serverAddress;
        private final int serverPort;
        private final int portToBind;
        private final CountDownLatch latch;
        private final CountDownLatch closeLatch;
        private final AtomicBoolean success;

        private PortBlockingRunnable(
            InetAddress serverAddress,
            int serverPort,
            int portToBind,
            CountDownLatch latch,
            CountDownLatch closeLatch,
            AtomicBoolean success
        ) {
            this.serverAddress = serverAddress;
            this.serverPort = serverPort;
            this.portToBind = portToBind;
            this.latch = latch;
            this.closeLatch = closeLatch;
            this.success = success;
        }

        @Override
        public void run() {
            final List<Socket> openedSockets = new ArrayList<>();
            final List<InetAddress> failedAddresses = new ArrayList<>();
            try {
                final boolean allSocketsOpened = waitUntil(() -> {
                    try {
                        final InetAddress[] allAddresses;
                        if (serverAddress instanceof Inet4Address) {
                            allAddresses = NetworkUtils.getAllIPV4Addresses();
                        } else {
                            allAddresses = NetworkUtils.getAllIPV6Addresses();
                        }
                        final List<InetAddress> inetAddressesToBind = Arrays.stream(allAddresses)
                            .filter(addr -> openedSockets.stream().noneMatch(s -> addr.equals(s.getLocalAddress())))
                            .filter(addr -> failedAddresses.contains(addr) == false)
                            .collect(Collectors.toList());
                        for (InetAddress localAddress : inetAddressesToBind) {
                            try {
                                final Socket socket = openMockSocket(serverAddress, serverPort, localAddress, portToBind);
                                openedSockets.add(socket);
                                logger.debug("opened socket [{}]", socket);
                            } catch (NoRouteToHostException | ConnectException e) {
                                logger.debug(new ParameterizedMessage("marking address [{}] as failed due to:", localAddress), e);
                                failedAddresses.add(localAddress);
                            }
                        }
                        if (openedSockets.size() == 0) {
                            logger.debug("Could not open any sockets from the available addresses");
                            return false;
                        }
                        return true;
                    } catch (IOException e) {
                        logger.debug(new ParameterizedMessage("caught exception while opening socket on [{}]", portToBind), e);
                        return false;
                    }
                });

                if (allSocketsOpened) {
                    latch.countDown();
                } else {
                    success.set(false);
                    IOUtils.closeWhileHandlingException(openedSockets);
                    openedSockets.clear();
                    latch.countDown();
                    return;
                }
            } catch (InterruptedException e) {
                logger.debug(new ParameterizedMessage("interrupted while trying to open sockets on [{}]", portToBind), e);
                Thread.currentThread().interrupt();
            }

            try {
                closeLatch.await();
            } catch (InterruptedException e) {
                logger.debug("caught exception while waiting for close latch", e);
                Thread.currentThread().interrupt();
            } finally {
                logger.debug("closing sockets on [{}]", portToBind);
                IOUtils.closeWhileHandlingException(openedSockets);
            }
        }
    }

    static class TestSessionFactory extends SessionFactory {

        protected TestSessionFactory(RealmConfig config, SSLService sslService, ThreadPool threadPool) {
            super(config, sslService, threadPool);
        }

        @Override
        public void session(String user, SecureString password, ActionListener<LdapSession> listener) {
            listener.onResponse(null);
        }
    }
}
