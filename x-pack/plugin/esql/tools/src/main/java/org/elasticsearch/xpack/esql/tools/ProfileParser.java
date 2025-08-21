/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tools;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProfileParser {

    public static void main(String[] args) throws IOException {
        PluginManager.addPackage(LogConfigurator.class.getPackage().getName());
        LogConfigurator.configureESLogging();
        Logger logger = LogManager.getLogger(ProfileParser.class);

        if (args.length != 2) {
            throw new IllegalArgumentException("Requires input and output file names");
        }
        // Enable using the tilde shorthand `~` for the home directory.
        Path inputFileName = Path.of(args[0].replaceFirst("^~", System.getProperty("user.home"))).toAbsolutePath();
        Path outputFileName = Path.of(args[1].replaceFirst("^~", System.getProperty("user.home"))).toAbsolutePath();

        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Profile profile;
        try (InputStream input = Files.newInputStream(inputFileName)) {
            logger.info("Starting to parse {}", inputFileName);
            profile = readProfileFromResponse(input);
            logger.info("Finished parsing", inputFileName);
        }

        try (
            OutputStream output = Files.newOutputStream(outputFileName);
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, output)
        ) {
            logger.info("Starting transformation into Chromium/Perfetto-compatible output format and writing to {}", outputFileName);
            parseProfile(profile, builder);
            logger.info("Finished writing to", outputFileName);
        }

        logger.info("Exiting", args[0]);
    }

    public record Response(Profile profile, @JsonProperty("is_partial") boolean isPartial, @JsonProperty("took") long took) {}

    public record Profile(List<Driver> drivers) {}

    public record Driver(
        @JsonProperty("description") String description,
        @JsonProperty("cluster_name") String clusterName,
        @JsonProperty("node_name") String nodeName,
        @JsonProperty("start_millis") long startMillis,
        @JsonProperty("stop_millis") long stopMillis,
        @JsonProperty("took_nanos") long tookNanos,
        @JsonProperty("cpu_nanos") long cpuNanos,
        @JsonProperty("iterations") int iterations,
        @JsonProperty("operators") List<Operator> operators,
        @JsonProperty("sleeps") Sleeps sleeps
    ) {}

    public record Operator(@JsonProperty("operator") String operator) {}

    public record Sleeps(@JsonProperty("counts") Map<String, Integer> counts) {}

    public static Profile readProfileFromResponse(InputStream input) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return jsonMapper.readValue(input, Response.class).profile();
    }

    /**
     * Parse the profile and transform it into Chromium's legacy profiling format:
     * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
     * We'd probably want to upgrade to the newer, more flexible protobuf-based spec by perfetto in the future - but this one also turned
     * out to be tricky to use.
     */
    @SuppressWarnings("unchecked")
    public static void parseProfile(Profile profile, XContentBuilder outputBuilder) throws IOException {
        outputBuilder.startObject();
        outputBuilder.field("displayTimeUnit", "ns");

        outputBuilder.field("traceEvents");
        outputBuilder.startArray();
        // We need to represent the nodes and drivers as processes and threads, resp., identified via integers pid and tid.
        // Let's keep track of them in maps.
        Map<String, Integer> nodeIndices = new HashMap<>();
        int driverIndex = 0;
        for (Driver driver : profile.drivers()) {
            String nodeName = driver.clusterName() + ":" + driver.nodeName();

            Integer nodeIndex = nodeIndices.get(nodeName);
            if (nodeIndex == null) {
                // New node encountered
                nodeIndex = nodeIndices.size();
                nodeIndices.put(nodeName, nodeIndex);

                emitMetadataForNode(nodeName, nodeIndex, outputBuilder);
            }

            // Represent each driver as a separate thread, but group them together into one process per node.
            parseDriverProfile(driver, nodeIndex, driverIndex++, outputBuilder);
        }
        outputBuilder.endArray();

        outputBuilder.endObject();
    }

    /**
     * Emit a metadata event for a new cluster node. We declare the node as a process, so we can group drivers by it.
     */
    private static void emitMetadataForNode(String nodeName, int pid, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("ph", "M");
        builder.field("name", "process_name");
        builder.field("pid", pid);

        builder.field("args");
        builder.startObject();
        builder.field("name", nodeName);
        builder.endObject();

        builder.endObject();
    }

    /**
     * Uses the legacy Chromium spec for event descriptions:
     * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
     *
     * Associates the driver with a given process and thread id to separate them visually.
     */
    @SuppressWarnings("unchecked")
    private static void parseDriverProfile(Driver driver, int pid, int tid, XContentBuilder builder) throws IOException {
        String driverDescription = driver.description();
        String name = driverDescription + " " + pid + ":" + tid;

        emitMetadataForDriver(driverDescription, pid, tid, builder);

        builder.startObject();
        // Represent a driver as a "complete" event, so that the cpu time can be represented visually.
        builder.field("ph", "X");
        builder.field("name", name);
        builder.field("cat", driverDescription);
        builder.field("pid", pid);
        builder.field("tid", tid);
        long startMicros = driver.startMillis() * 1000;
        builder.field("ts", startMicros);
        double durationMicros = ((double) driver.tookNanos()) / 1000.0;
        builder.field("dur", durationMicros);
        double cpuDurationMicros = ((double) driver.cpuNanos()) / 1000.0;
        builder.field("tdur", cpuDurationMicros);

        builder.field("args");
        builder.startObject();
        builder.field("cpu_nanos", driver.cpuNanos());
        builder.field("took_nanos", driver.tookNanos());
        builder.field("iterations", driver.iterations());
        // TODO: Sleeps have more details that could be added here
        int totalSleeps = driver.sleeps().counts().values().stream().reduce(0, Integer::sum);
        builder.field("sleeps", totalSleeps);
        builder.field("operators");
        builder.startArray();
        for (Operator operator : driver.operators()) {
            builder.value(operator.operator());
            // TODO: Add status; needs standardizing the operatur statuses, maybe.
        }
        builder.endArray();
        builder.endObject();

        builder.endObject();
    }

    /**
     * Emit a metadata event for a new driver. We declare the driver as a thread.
     */
    private static void emitMetadataForDriver(String driverName, int pid, int tid, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("ph", "M");
        builder.field("name", "thread_name");
        builder.field("pid", pid);
        builder.field("tid", tid);

        builder.field("args");
        builder.startObject();
        builder.field("name", driverName);
        builder.endObject();

        builder.endObject();
    }
}
