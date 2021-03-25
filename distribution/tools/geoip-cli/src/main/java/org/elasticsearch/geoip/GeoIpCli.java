/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geoip;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class GeoIpCli extends Command {

    private static final byte[] EMPTY_BUF = new byte[512];
    private final OptionSpec<String> directoryArg;

    public GeoIpCli() {
        super("A CLI tool to prepare ", () -> {
        });
        directoryArg = parser.acceptsAll(Arrays.asList("d", "directory"), "Directory to process").withRequiredArg().defaultsTo(".");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        Path directory = Paths.get(options.valueOf(directoryArg));
        List<Path> toPack = Files.list(directory)
            .filter(p -> p.getFileName().toString().endsWith(".mmdb"))
            .collect(Collectors.toList());
        for (Path path : toPack) {
            String fileName = path.getFileName().toString();
            Path compressedPath = path.resolveSibling(fileName.replaceAll("mmdb$", "") + "tgz");
            try (OutputStream fos = Files.newOutputStream(compressedPath, TRUNCATE_EXISTING, CREATE);
                 OutputStream gos = new GZIPOutputStream(new BufferedOutputStream(fos))) {
                long size = Files.size(path);
                gos.write(createTarHeader(fileName, size));
                Files.copy(path, gos);
                if (size % 512 != 0) {
                    gos.write(EMPTY_BUF, 0, (int) (512 - (size % 512)));
                }
                gos.write(EMPTY_BUF);
                gos.write(EMPTY_BUF);
            }
        }
        List<Path> databasesPaths = Files.list(directory)
            .filter(p -> p.getFileName().toString().endsWith(".tgz"))
            .collect(Collectors.toList());
        Path overview = directory.resolve("overview.json");
        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(overview, TRUNCATE_EXISTING, CREATE));
             XContentGenerator generator = XContentType.JSON.xContent().createGenerator(os)) {
            generator.writeStartArray();
            for (Path db : databasesPaths) {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                byte[] buf = new byte[4096];
                try (InputStream dis = new DigestInputStream(new BufferedInputStream(Files.newInputStream(db)), md5)) {
                    while (dis.read(buf) != -1) {
                    }
                }
                String digest = toHexString(md5.digest());
                generator.writeStartObject();
                String fileName = db.getFileName().toString();
                generator.writeStringField("name", fileName);
                generator.writeStringField("md5_hash", digest);
                generator.writeStringField("url", fileName);
                generator.writeNumberField("updated", System.currentTimeMillis());
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }
    }

    private byte[] createTarHeader(String name, long size) {
        byte[] buf = new byte[512];
        byte[] sizeBytes = String.format(Locale.ROOT, "%1$011o", size).getBytes(StandardCharsets.UTF_8);
        byte[] nameBytes = name.substring(Math.max(0, name.length() - 98)).getBytes(StandardCharsets.US_ASCII);
        byte[] id = "0001750".getBytes(StandardCharsets.UTF_8);
        byte[] permission = "000644 ".getBytes(StandardCharsets.UTF_8);
        byte[] magic = "ustar".getBytes(StandardCharsets.UTF_8);
        byte[] time = String.format(Locale.ROOT, "%1$011o", System.currentTimeMillis() / 1000).getBytes(StandardCharsets.UTF_8);
        buf[156] = '0';
        buf[263] = '0';
        buf[264] = '0';
        System.arraycopy(nameBytes, 0, buf, 0, nameBytes.length);
        System.arraycopy(permission, 0, buf, 100, 7);
        System.arraycopy(id, 0, buf, 108, 7);
        System.arraycopy(id, 0, buf, 116, 7);
        System.arraycopy(sizeBytes, 0, buf, 124, 11);
        System.arraycopy(time, 0, buf, 136, 11);
        System.arraycopy(magic, 0, buf, 257, 5);

        int checksum = 256;
        for (byte b : buf) {
            checksum += b & 0xFF;
        }

        byte[] checksumBytes = String.format(Locale.ROOT, "%1$07o", checksum).getBytes(StandardCharsets.UTF_8);
        System.arraycopy(checksumBytes, 0, buf, 148, 7);

        return buf;
    }

    public static void main(String[] args) throws Exception {
        exit(new GeoIpCli().main(args, Terminal.DEFAULT));
    }

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    /**
     * Format a byte array as a hex string.
     *
     * @param bytes the input to be represented as hex.
     * @return a hex representation of the input as a String.
     */
    public static String toHexString(byte[] bytes) {
        return new String(toHexCharArray(bytes));
    }

    /**
     * Encodes the byte array into a newly created hex char array, without allocating any other temporary variables.
     *
     * @param bytes the input to be encoded as hex.
     * @return the hex encoding of the input as a char array.
     */
    public static char[] toHexCharArray(byte[] bytes) {
        Objects.requireNonNull(bytes);
        final char[] result = new char[2 * bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            result[2 * i] = HEX_DIGITS[b >> 4 & 0xf];
            result[2 * i + 1] = HEX_DIGITS[b & 0xf];
        }
        return result;
    }
}
