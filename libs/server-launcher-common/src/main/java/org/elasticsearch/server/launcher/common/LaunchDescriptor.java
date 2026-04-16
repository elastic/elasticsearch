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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Holds all the information needed by the launcher to spawn the Elasticsearch server process.
 * <p>
 * This class is serialized to a binary format by the preparer (server-cli) and deserialized
 * by the launcher (server-launcher). It uses only JDK classes so that the launcher has no
 * Elasticsearch dependencies.
 * <p>
 * Subclasses (e.g. {@code ServerlessLaunchDescriptor}) can extend this class and use
 * {@link #writeFieldsTo(DataOutputStream)} / {@link #readFieldsFrom(DataInputStream)}
 * to serialize/deserialize base fields without the magic number prefix.
 */
public class LaunchDescriptor {

    public static final String DESCRIPTOR_FILENAME = "launch-descriptor.bin";

    private static final int MAGIC = 0x45534C44; // "ESLD" - ElasticSearch Launch Descriptor

    private final String command;
    private final List<String> jvmOptions;
    private final List<String> jvmArgs;
    private final Map<String, String> environment;
    private final String workingDir;
    private final String tempDir;
    private final boolean daemonize;
    private final byte[] serverArgsBytes;

    public LaunchDescriptor(
        String command,
        List<String> jvmOptions,
        List<String> jvmArgs,
        Map<String, String> environment,
        String workingDir,
        String tempDir,
        boolean daemonize,
        byte[] serverArgsBytes
    ) {
        this.command = command;
        this.jvmOptions = List.copyOf(jvmOptions);
        this.jvmArgs = List.copyOf(jvmArgs);
        this.environment = Map.copyOf(environment);
        this.workingDir = workingDir;
        this.tempDir = tempDir;
        this.daemonize = daemonize;
        this.serverArgsBytes = serverArgsBytes.clone();
    }

    /**
     * Copy constructor for use by subclasses.
     */
    protected LaunchDescriptor(LaunchDescriptor other) {
        this.command = other.command;
        this.jvmOptions = other.jvmOptions;
        this.jvmArgs = other.jvmArgs;
        this.environment = other.environment;
        this.workingDir = other.workingDir;
        this.tempDir = other.tempDir;
        this.daemonize = other.daemonize;
        this.serverArgsBytes = other.serverArgsBytes;
    }

    public String command() {
        return command;
    }

    public List<String> jvmOptions() {
        return jvmOptions;
    }

    public List<String> jvmArgs() {
        return jvmArgs;
    }

    public Map<String, String> environment() {
        return environment;
    }

    public String workingDir() {
        return workingDir;
    }

    public String tempDir() {
        return tempDir;
    }

    public boolean daemonize() {
        return daemonize;
    }

    public byte[] serverArgsBytes() {
        return serverArgsBytes;
    }

    /**
     * Writes this descriptor to a binary file.
     */
    public void writeTo(Path path) throws IOException {
        try (OutputStream fos = Files.newOutputStream(path); DataOutputStream out = new DataOutputStream(fos)) {
            writeTo(out);
        }
    }

    /**
     * Writes this descriptor to a DataOutputStream, including the magic number prefix.
     */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(MAGIC);
        writeFieldsTo(out);
        out.flush();
    }

    /**
     * Writes the fields of this descriptor without the magic number prefix.
     * Subclasses can call this to embed base fields in their own wire format.
     */
    protected void writeFieldsTo(DataOutputStream out) throws IOException {
        out.writeUTF(command);
        writeStringList(out, jvmOptions);
        writeStringList(out, jvmArgs);
        writeStringMap(out, environment);
        out.writeUTF(workingDir);
        out.writeUTF(tempDir);
        out.writeBoolean(daemonize);
        out.writeInt(serverArgsBytes.length);
        out.write(serverArgsBytes);
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
     * Reads a descriptor from a DataInputStream, checking the magic number prefix.
     */
    public static LaunchDescriptor readFrom(DataInputStream in) throws IOException {
        checkMagic(in, MAGIC);
        return readFieldsFrom(in);
    }

    /**
     * Reads descriptor fields without the magic number prefix.
     * Subclasses can call this to read base fields from their own wire format.
     */
    protected static LaunchDescriptor readFieldsFrom(DataInputStream in) throws IOException {
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
     * Checks the magic number from the stream and throws if it doesn't match.
     */
    protected static void checkMagic(DataInputStream in, int expectedMagic) throws IOException {
        int magic = in.readInt();
        if (magic != expectedMagic) {
            throw new IOException("Invalid launch descriptor: bad magic number");
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LaunchDescriptor that = (LaunchDescriptor) o;
        return daemonize == that.daemonize
            && Objects.equals(command, that.command)
            && Objects.equals(jvmOptions, that.jvmOptions)
            && Objects.equals(jvmArgs, that.jvmArgs)
            && Objects.equals(environment, that.environment)
            && Objects.equals(workingDir, that.workingDir)
            && Objects.equals(tempDir, that.tempDir)
            && Arrays.equals(serverArgsBytes, that.serverArgsBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(command, jvmOptions, jvmArgs, environment, workingDir, tempDir, daemonize);
        result = 31 * result + Arrays.hashCode(serverArgsBytes);
        return result;
    }

    @Override
    public String toString() {
        return "LaunchDescriptor{command='" + command + "', workingDir='" + workingDir + "', daemonize=" + daemonize + "}";
    }

    protected static void writeNullableString(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeUTF(value);
        }
    }

    protected static String readNullableString(DataInputStream in) throws IOException {
        boolean present = in.readBoolean();
        return present ? in.readUTF() : null;
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
