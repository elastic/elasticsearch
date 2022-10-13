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
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class GeoIpCli extends Command {

    private static final byte[] EMPTY_BUF = new byte[512];

    private final OptionSpec<String> sourceDirectory;
    private final OptionSpec<String> targetDirectory;

    public GeoIpCli() {
        super("A CLI tool to prepare local GeoIp database service");
        sourceDirectory = parser.acceptsAll(Arrays.asList("s", "source"), "Source directory").withRequiredArg().required();
        targetDirectory = parser.acceptsAll(Arrays.asList("t", "target"), "Target directory").withRequiredArg();

    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        Path source = getPath(options.valueOf(sourceDirectory));
        String targetString = options.valueOf(targetDirectory);
        Path target = targetString != null ? getPath(targetString) : source;
        copyTgzToTarget(source, target);
        packDatabasesToTgz(terminal, source, target);
        createOverviewJson(terminal, target);
    }

    @SuppressForbidden(reason = "file arg for cli")
    private Path getPath(String file) {
        return PathUtils.get(file);
    }

    private void copyTgzToTarget(Path source, Path target) throws IOException {
        if (source.equals(target)) {
            return;
        }
        try (Stream<Path> files = Files.list(source)) {
            for (Path path : files.filter(p -> p.getFileName().toString().endsWith(".tgz")).collect(Collectors.toList())) {
                Files.copy(path, target.resolve(path.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private void packDatabasesToTgz(Terminal terminal, Path source, Path target) throws IOException {
        try (Stream<Path> files = Files.list(source)) {
            for (Path path : files.filter(p -> p.getFileName().toString().endsWith(".mmdb")).collect(Collectors.toList())) {
                String fileName = path.getFileName().toString();
                Path compressedPath = target.resolve(fileName.replaceAll("mmdb$", "") + "tgz");
                terminal.println("Found " + fileName + ", will compress it to " + compressedPath.getFileName());
                try (
                    OutputStream fos = Files.newOutputStream(compressedPath, TRUNCATE_EXISTING, CREATE);
                    OutputStream gos = new GZIPOutputStream(new BufferedOutputStream(fos))
                ) {
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
        }
    }

    private void createOverviewJson(Terminal terminal, Path directory) throws IOException {
        Path overview = directory.resolve("overview.json");
        try (
            Stream<Path> files = Files.list(directory);
            OutputStream os = new BufferedOutputStream(Files.newOutputStream(overview, TRUNCATE_EXISTING, CREATE));
            XContentGenerator generator = XContentType.JSON.xContent().createGenerator(os)
        ) {
            generator.writeStartArray();
            for (Path db : files.filter(p -> p.getFileName().toString().endsWith(".tgz")).collect(Collectors.toList())) {
                terminal.println("Adding " + db.getFileName() + " to overview.json");
                MessageDigest md5 = MessageDigests.md5();
                try (InputStream dis = new DigestInputStream(new BufferedInputStream(Files.newInputStream(db)), md5)) {
                    dis.transferTo(OutputStream.nullOutputStream());
                }
                String digest = MessageDigests.toHexString(md5.digest());
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
        terminal.println("overview.json created");
    }

    private byte[] createTarHeader(String name, long size) {
        byte[] buf = new byte[512];
        byte[] sizeBytes = String.format(Locale.ROOT, "%1$012o", size).getBytes(StandardCharsets.UTF_8);
        byte[] nameBytes = name.substring(Math.max(0, name.length() - 100)).getBytes(StandardCharsets.US_ASCII);
        byte[] id = "0001750".getBytes(StandardCharsets.UTF_8);
        byte[] permission = "000644 ".getBytes(StandardCharsets.UTF_8);
        byte[] time = String.format(Locale.ROOT, "%1$012o", System.currentTimeMillis() / 1000).getBytes(StandardCharsets.UTF_8);
        System.arraycopy(nameBytes, 0, buf, 0, nameBytes.length);
        System.arraycopy(permission, 0, buf, 100, 7);
        System.arraycopy(id, 0, buf, 108, 7);
        System.arraycopy(id, 0, buf, 116, 7);
        System.arraycopy(sizeBytes, 0, buf, 124, 12);
        System.arraycopy(time, 0, buf, 136, 12);

        int checksum = 256;
        for (byte b : buf) {
            checksum += b & 0xFF;
        }

        byte[] checksumBytes = String.format(Locale.ROOT, "%1$07o", checksum).getBytes(StandardCharsets.UTF_8);
        System.arraycopy(checksumBytes, 0, buf, 148, 7);

        return buf;
    }
}
