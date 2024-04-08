/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.url;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.fixture.AbstractHttpFixture;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This {@link URLFixture} exposes a filesystem directory over HTTP. It is used in repository-url
 * integration tests to expose a directory created by a regular FS repository.
 */
public class URLFixture extends AbstractHttpFixture implements TestRule {
    private static final Pattern RANGE_PATTERN = Pattern.compile("bytes=(\\d+)-(\\d+)$");
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
                final String range = request.getHeader("Range");
                final Map<String, String> headers = new HashMap<>(contentType("application/octet-stream"));
                if (range == null) {
                    byte[] content = Files.readAllBytes(normalizedPath);
                    headers.put("Content-Length", String.valueOf(content.length));
                    return new Response(RestStatus.OK.getStatus(), headers, content);
                } else {
                    final Matcher matcher = RANGE_PATTERN.matcher(range);
                    if (matcher.matches() == false) {
                        return new Response(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                    } else {
                        long start = Long.parseLong(matcher.group(1));
                        long end = Long.parseLong(matcher.group(2));
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
        InetSocketAddress inetSocketAddress = resolveAddress("0.0.0.0", 0);
        listen(inetSocketAddress, false);
    }

    public String getRepositoryDir() {
        if (repositoryDir == null) {
            throw new IllegalStateException("Rule has not been started yet");
        }
        return repositoryDir.toFile().getAbsolutePath();
    }

    private static InetSocketAddress resolveAddress(String address, int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName(address), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void after() {
        super.stop();
        this.temporaryFolder.delete();
    }
}
