/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.url;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.fixture.AbstractHttpFixture;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This {@link URLFixture} exposes a filesystem directory over HTTP. It is used in repository-url
 * integration tests to expose a directory created by a regular FS repository.
 */
public class URLFixture extends AbstractHttpFixture {
    private static final Pattern RANGE_PATTERN = Pattern.compile("bytes=(\\d+)-(\\d+)$");
    private final Path repositoryDir;

    /**
     * Creates a {@link URLFixture}
     */
    private URLFixture(final int port, final String workingDir, final String repositoryDir) {
        super(workingDir, port);
        this.repositoryDir = dir(repositoryDir);
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 3) {
            throw new IllegalArgumentException("URLFixture <port> <working directory> <repository directory>");
        }
        String workingDirectory = args[1];
        if (Files.exists(dir(workingDirectory)) == false) {
            throw new IllegalArgumentException("Configured working directory " + workingDirectory + " does not exist");
        }
        String repositoryDirectory = args[2];
        if (Files.exists(dir(repositoryDirectory)) == false) {
            throw new IllegalArgumentException("Configured repository directory " + repositoryDirectory + " does not exist");
        }
        final URLFixture fixture = new URLFixture(Integer.parseInt(args[0]), workingDirectory, repositoryDirectory);
        fixture.listen(InetAddress.getByName("0.0.0.0"), false);
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

    @SuppressForbidden(reason = "Paths#get is fine - we don't have environment here")
    private static Path dir(final String dir) {
        return Paths.get(dir);
    }
}
