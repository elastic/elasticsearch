/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds all the information needed by the launcher to spawn the Elasticsearch server process.
 * <p>
 * This record is serialized to a binary file by the preparer (server-cli) and deserialized
 * by the launcher (server-launcher). It uses only JDK classes so that the launcher has no
 * Elasticsearch dependencies.
 *
 * @param command        path to the java binary
 * @param jvmOptions     JVM flags (e.g. -Xms4g, -XX:+UseG1GC)
 * @param jvmArgs        module-path, -m, and other JVM arguments
 * @param environment    environment variables for the server process
 * @param workingDir     working directory for the server process (logs directory)
 * @param tempDir        path to the temp directory
 * @param daemonize      whether the server should be daemonized
 * @param serverArgsBytes opaque blob of serialized ServerArgs (piped to the server's stdin)
 */
public record LaunchDescriptor(
    String command,
    List<String> jvmOptions,
    List<String> jvmArgs,
    Map<String, String> environment,
    String workingDir,
    String tempDir,
    boolean daemonize,
    byte[] serverArgsBytes
) {

    public static final String DESCRIPTOR_FILENAME = "launch-descriptor.bin";

    private static final int MAGIC = 0x45534C44; // "ESLD" - ElasticSearch Launch Descriptor

    /**
     * Writes this descriptor to a binary file.
     */
    public void writeTo(Path path) throws IOException {
        try (OutputStream fos = Files.newOutputStream(path); DataOutputStream out = new DataOutputStream(fos)) {
            writeTo(out);
        }
    }

    /**
     * Writes this descriptor to a DataOutputStream.
     */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(MAGIC);

        out.writeUTF(command);
        writeStringList(out, jvmOptions);
        writeStringList(out, jvmArgs);
        writeStringMap(out, environment);
        out.writeUTF(workingDir);
        out.writeUTF(tempDir);
        out.writeBoolean(daemonize);
        out.writeInt(serverArgsBytes.length);
        out.write(serverArgsBytes);

        out.flush();
    }

    /**
     * Reads a descriptor from a binary file.
     */
    public static LaunchDescriptor readFrom(Path path) throws IOException {
        try (InputStream fis = Files.newInputStream(path); DataInputStream in = new DataInputStream(fis)) {
            return readFrom(in);
        }
    }

    /**
     * Reads a descriptor from a DataInputStream.
     */
    public static LaunchDescriptor readFrom(DataInputStream in) throws IOException {
        int magic = in.readInt();
        if (magic != MAGIC) {
            throw new IOException("Invalid launch descriptor: bad magic number");
        }

        String command = in.readUTF();
        List<String> jvmOptions = readStringList(in);
        List<String> jvmArgs = readStringList(in);
        Map<String, String> environment = readStringMap(in);
        String workingDir = in.readUTF();
        String tempDir = in.readUTF();
        boolean daemonize = in.readBoolean();
        int serverArgsBytesLen = in.readInt();
        byte[] serverArgsBytes = in.readNBytes(serverArgsBytesLen);
        if (serverArgsBytes.length != serverArgsBytesLen) {
            throw new IOException("Truncated server args: expected " + serverArgsBytesLen + " bytes, got " + serverArgsBytes.length);
        }

        return new LaunchDescriptor(command, jvmOptions, jvmArgs, environment, workingDir, tempDir, daemonize, serverArgsBytes);
    }

    /**
     * Returns a human-readable representation of this descriptor in a section-based text format.
     * Useful for debugging with the launcher's --dump flag.
     */
    public String toHumanReadable() {
        StringBuilder sb = new StringBuilder();

        appendSection(sb, "command", command);
        appendSection(sb, "working_dir", workingDir);
        appendSection(sb, "temp_dir", tempDir);
        appendSection(sb, "daemonize", String.valueOf(daemonize));
        appendSection(sb, "server_args_bytes", "<" + serverArgsBytes.length + " bytes>");

        sb.append("[jvm_options]\n");
        for (String opt : jvmOptions) {
            sb.append(opt).append('\n');
        }
        sb.append('\n');

        sb.append("[jvm_args]\n");
        for (String arg : jvmArgs) {
            sb.append(arg).append('\n');
        }
        sb.append('\n');

        sb.append("[environment]\n");
        for (Map.Entry<String, String> entry : environment.entrySet()) {
            sb.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }

        return sb.toString();
    }

    private static void appendSection(StringBuilder sb, String name, String value) {
        sb.append('[').append(name).append("]\n");
        sb.append(value).append("\n\n");
    }

    private static void writeStringList(DataOutputStream out, List<String> list) throws IOException {
        out.writeInt(list.size());
        for (String s : list) {
            out.writeUTF(s);
        }
    }

    private static List<String> readStringList(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(in.readUTF());
        }
        return list;
    }

    private static void writeStringMap(DataOutputStream out, Map<String, String> map) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    private static Map<String, String> readStringMap(DataInputStream in) throws IOException {
        int size = in.readInt();
        Map<String, String> map = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            map.put(key, value);
        }
        return map;
    }
}
