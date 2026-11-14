/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Reads single entries out of a zip archive served over HTTP, using range requests for the central directory and
 * for each requested entry instead of downloading the whole archive.
 *
 * <p>DRA snapshot builds publish their Maven artifacts only inside a ~250 MB
 * {@code elasticsearch-maven-aggregation-<version>-SNAPSHOT.zip}. bwc tests need four small JARs from it, and need
 * them from the same build as the pinned distribution, so this reads just the central directory and those entries.
 * Servers that ignore {@code Range} and send the whole body are tolerated. Zip64 archives are not supported.
 */
final class RemoteZipEntryReader {

    private static final int EOCD_SIGNATURE = 0x06054b50;
    private static final int CENTRAL_HEADER_SIGNATURE = 0x02014b50;
    private static final int LOCAL_HEADER_SIGNATURE = 0x04034b50;
    private static final int EOCD_MIN_SIZE = 22;
    private static final int MAX_EOCD_SEARCH = EOCD_MIN_SIZE + 0xFFFF;
    private static final int LOCAL_HEADER_SIZE = 30;
    private static final long ZIP64_MARKER = 0xFFFFFFFFL;

    private record Entry(int method, long compressedSize, long size, long localHeaderOffset) {}

    private final HttpClient client;
    private final String url;
    private final Map<String, Entry> entries;

    private RemoteZipEntryReader(HttpClient client, String url, Map<String, Entry> entries) {
        this.client = client;
        this.url = url;
        this.entries = entries;
    }

    /** Fetches and parses the archive's central directory. */
    static RemoteZipEntryReader open(HttpClient client, String url) throws IOException {
        long length = contentLength(client, url);
        int tailLength = (int) Math.min(length, MAX_EOCD_SEARCH);
        ByteBuffer tail = order(range(client, url, length - tailLength, tailLength));
        int eocd = -1;
        for (int i = tailLength - EOCD_MIN_SIZE; i >= 0; i--) {
            if (tail.getInt(i) == EOCD_SIGNATURE) {
                eocd = i;
                break;
            }
        }
        if (eocd < 0) {
            throw new IOException("[" + url + "] is not a zip archive: end of central directory record not found");
        }
        long directorySize = Integer.toUnsignedLong(tail.getInt(eocd + 12));
        long directoryOffset = Integer.toUnsignedLong(tail.getInt(eocd + 16));
        if (directorySize == ZIP64_MARKER || directoryOffset == ZIP64_MARKER) {
            throw new IOException("[" + url + "] is a zip64 archive, which is not supported");
        }

        ByteBuffer directory = order(range(client, url, directoryOffset, (int) directorySize));
        Map<String, Entry> entries = new LinkedHashMap<>();
        int position = 0;
        while (position + 46 <= directorySize && directory.getInt(position) == CENTRAL_HEADER_SIGNATURE) {
            int method = Short.toUnsignedInt(directory.getShort(position + 10));
            long compressedSize = Integer.toUnsignedLong(directory.getInt(position + 20));
            long size = Integer.toUnsignedLong(directory.getInt(position + 24));
            int nameLength = Short.toUnsignedInt(directory.getShort(position + 28));
            int extraLength = Short.toUnsignedInt(directory.getShort(position + 30));
            int commentLength = Short.toUnsignedInt(directory.getShort(position + 32));
            long localHeaderOffset = Integer.toUnsignedLong(directory.getInt(position + 42));
            byte[] name = new byte[nameLength];
            directory.get(position + 46, name);
            entries.put(new String(name, StandardCharsets.UTF_8), new Entry(method, compressedSize, size, localHeaderOffset));
            position += 46 + nameLength + extraLength + commentLength;
        }
        return new RemoteZipEntryReader(client, url, entries);
    }

    /** All entry names, in central directory order. */
    List<String> entryNames() {
        return new ArrayList<>(entries.keySet());
    }

    /** Returns the uncompressed content of {@code name}. */
    byte[] read(String name) throws IOException {
        Entry entry = entries.get(name);
        if (entry == null) {
            throw new IOException("entry [" + name + "] not found in [" + url + "]");
        }
        if (entry.compressedSize() == ZIP64_MARKER || entry.size() == ZIP64_MARKER || entry.localHeaderOffset() == ZIP64_MARKER) {
            throw new IOException("entry [" + name + "] in [" + url + "] uses zip64, which is not supported");
        }
        ByteBuffer header = order(range(client, url, entry.localHeaderOffset(), LOCAL_HEADER_SIZE));
        if (header.getInt(0) != LOCAL_HEADER_SIGNATURE) {
            throw new IOException("corrupt local header for entry [" + name + "] in [" + url + "]");
        }
        long dataOffset = entry.localHeaderOffset() + LOCAL_HEADER_SIZE + Short.toUnsignedInt(header.getShort(26)) + Short.toUnsignedInt(
            header.getShort(28)
        );
        byte[] data = range(client, url, dataOffset, (int) entry.compressedSize());
        return switch (entry.method()) {
            case 0 -> data;
            case 8 -> inflate(name, data, (int) entry.size());
            default -> throw new IOException("entry [" + name + "] in [" + url + "] uses unsupported compression method " + entry.method());
        };
    }

    private byte[] inflate(String name, byte[] data, int size) throws IOException {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(data);
            ByteArrayOutputStream out = new ByteArrayOutputStream(size);
            byte[] buffer = new byte[64 * 1024];
            while (inflater.finished() == false) {
                int n = inflater.inflate(buffer);
                if (n == 0 && (inflater.needsInput() || inflater.needsDictionary())) {
                    break;
                }
                out.write(buffer, 0, n);
            }
            if (out.size() != size) {
                throw new IOException("entry [" + name + "] in [" + url + "] inflated to " + out.size() + " bytes, expected " + size);
            }
            return out.toByteArray();
        } catch (DataFormatException e) {
            throw new IOException("entry [" + name + "] in [" + url + "] is corrupt", e);
        } finally {
            inflater.end();
        }
    }

    private static long contentLength(HttpClient client, String url) throws IOException {
        HttpResponse<Void> response = send(
            client,
            HttpRequest.newBuilder(URI.create(url)).method("HEAD", HttpRequest.BodyPublishers.noBody()),
            HttpResponse.BodyHandlers.discarding()
        );
        if (response.statusCode() != 200) {
            throw new IOException("HEAD [" + url + "] returned HTTP " + response.statusCode());
        }
        return response.headers()
            .firstValueAsLong("Content-Length")
            .orElseThrow(() -> new IOException("HEAD [" + url + "] returned no Content-Length"));
    }

    private static byte[] range(HttpClient client, String url, long start, int length) throws IOException {
        if (length == 0) {
            return new byte[0];
        }
        long end = start + length - 1;
        HttpResponse<byte[]> response = send(
            client,
            HttpRequest.newBuilder(URI.create(url)).header("Range", "bytes=" + start + "-" + end).GET(),
            HttpResponse.BodyHandlers.ofByteArray()
        );
        byte[] body = response.body();
        if (response.statusCode() == 206 && body.length == length) {
            return body;
        }
        if (response.statusCode() == 200 && body.length > end) {
            // The server ignored the range and sent the whole archive.
            return Arrays.copyOfRange(body, (int) start, (int) end + 1);
        }
        throw new IOException(
            "GET [" + url + "] bytes " + start + "-" + end + " returned HTTP " + response.statusCode() + " with " + body.length + " bytes"
        );
    }

    private static <T> HttpResponse<T> send(HttpClient client, HttpRequest.Builder request, HttpResponse.BodyHandler<T> handler)
        throws IOException {
        try {
            return client.send(request.timeout(Duration.ofSeconds(60)).build(), handler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while fetching [" + request.build().uri() + "]", e);
        }
    }

    private static ByteBuffer order(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}
