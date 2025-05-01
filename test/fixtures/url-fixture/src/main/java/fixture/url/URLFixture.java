/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.url;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.fixture.AbstractHttpFixture;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * This {@link URLFixture} exposes a filesystem directory over HTTP. It is used in repository-url
 * integration tests to expose a directory created by a regular FS repository.
 */
public class URLFixture extends AbstractHttpFixture implements TestRule {
    private final TemporaryFolder temporaryFolder;
    private Path repositoryDir;

    /**
     * Creates a {@link URLFixture}
     */
    public URLFixture() {
        super();
        this.temporaryFolder = new TemporaryFolder();
    }

    @Override
    protected AbstractHttpFixture.Response handle(final Request request) throws IOException {
        if ("GET".equalsIgnoreCase(request.getMethod())) {
            return handleGetRequest(request);
        }
        return null;
    }

    private AbstractHttpFixture.Response handleGetRequest(Request request) throws IOException {
        String path = request.getPath();
        if (path.length() > 0 && path.charAt(0) == '/') {
            path = path.substring(1);
        }

        Path normalizedRepositoryDir = repositoryDir.normalize();
        Path normalizedPath = normalizedRepositoryDir.resolve(path).normalize();

        if (normalizedPath.startsWith(normalizedRepositoryDir)) {
            if (Files.exists(normalizedPath) && Files.isReadable(normalizedPath) && Files.isRegularFile(normalizedPath)) {
                final String rangeHeader = request.getHeader("Range");
                final Map<String, String> headers = new HashMap<>(contentType("application/octet-stream"));
                if (rangeHeader == null) {
                    byte[] content = Files.readAllBytes(normalizedPath);
                    headers.put("Content-Length", String.valueOf(content.length));
                    return new Response(RestStatus.OK.getStatus(), headers, content);
                } else {
                    final HttpHeaderParser.Range range = HttpHeaderParser.parseRangeHeader(rangeHeader);
                    if (range == null) {
                        return new Response(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                    } else {
                        long start = range.start();
                        long end = range.end();
                        long rangeLength = end - start + 1;
                        final long fileSize = Files.size(normalizedPath);
                        if (start >= fileSize || start > end || rangeLength > fileSize) {
                            return new Response(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                        }

                        headers.put("Content-Length", String.valueOf(rangeLength));
                        headers.put("Content-Range", "bytes " + start + "-" + end + "/" + fileSize);
                        final byte[] content = Files.readAllBytes(normalizedPath);
                        final byte[] responseData = new byte[(int) rangeLength];
                        System.arraycopy(content, (int) start, responseData, 0, (int) rangeLength);
                        return new Response(RestStatus.PARTIAL_CONTENT.getStatus(), headers, responseData);
                    }
                }
            } else {
                return new Response(RestStatus.NOT_FOUND.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
            }
        } else {
            return new Response(RestStatus.FORBIDDEN.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
        }
    }

    @Override
    protected void before() throws Throwable {
        this.temporaryFolder.create();
        this.repositoryDir = temporaryFolder.newFolder("repoDir").toPath();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        listen(inetSocketAddress, false);

        startFtpServer();
    }

    @Nullable // if not started
    private FtpServer ftpServer;

    @Nullable // if not started
    private String ftpUrl;

    private void startFtpServer() throws FtpException {
        final var listenerFactory = new ListenerFactory();
        listenerFactory.setServerAddress(InetAddress.getLoopbackAddress().getHostAddress());
        listenerFactory.setPort(0);
        final var listener = listenerFactory.createListener();
        final var listenersMap = new HashMap<String, Listener>();
        listenersMap.put("default", listener);

        final var user = new BaseUser();
        user.setName(ESTestCase.randomIdentifier());
        user.setPassword(ESTestCase.randomSecretKey());
        user.setEnabled(true);
        user.setHomeDirectory(getRepositoryDir());
        user.setMaxIdleTime(0);

        final var ftpServerFactory = new FtpServerFactory();
        ftpServerFactory.setListeners(listenersMap);
        ftpServerFactory.getUserManager().save(user);
        ftpServer = ftpServerFactory.createServer();
        ftpServer.start();
        ftpUrl = "ftp://" + user.getName() + ":" + user.getPassword() + "@" + listener.getServerAddress() + ":" + listener.getPort();
    }

    public String getFtpUrl() {
        return ftpUrl;
    }

    public String getRepositoryDir() {
        if (repositoryDir == null) {
            throw new IllegalStateException("Rule has not been started yet");
        }
        return repositoryDir.toFile().getAbsolutePath();
    }

    @Override
    protected void after() {
        if (ftpServer != null) {
            ftpServer.stop();
        }
        super.stop();
        this.temporaryFolder.delete();
    }
}
