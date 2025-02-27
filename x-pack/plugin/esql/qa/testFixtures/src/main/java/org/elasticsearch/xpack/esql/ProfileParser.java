/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.xcontent.XContentHelper;
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

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException {
        PluginManager.addPackage(LogConfigurator.class.getPackage().getName());
        LogConfigurator.configureESLogging();
        Logger logger = LogManager.getLogger(ProfileParser.class);

        if (args.length != 2) {
            throw new IllegalArgumentException("Requires input and output file names");
        }
        Path inputFileName = Path.of(args[0].replaceFirst("^~", System.getProperty("user.home"))).toAbsolutePath();
        Path outputFileName = Path.of(args[1].replaceFirst("^~", System.getProperty("user.home"))).toAbsolutePath();

        Map<String, Object> map;
        try (InputStream input = Files.newInputStream(inputFileName)) {
            logger.info("Starting to parse {}", inputFileName);
            map = XContentHelper.convertToMap(JsonXContent.jsonXContent, input, true);
            logger.info("Finished parsing", inputFileName);
        }

        Map<String, Object> profile = (Map<String, Object>) map.get("profile");
        List<Map<String, Object>> drivers = (List<Map<String, Object>>) profile.get("drivers");

        logger.info("Starting transformation into perfetto-compatible output format", args[0]);
        try (
            OutputStream output = Files.newOutputStream(outputFileName);
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, output)
        ) {
            logger.info("Starting to write {}", outputFileName);

            builder.startObject();
            builder.field("displayTimeUnit", "ns");

            builder.field("traceEvents");
            builder.startArray();
            // We need to represent the nodes and drivers as processes and threads, resp., identified via integers pid and tid.
            // Let's keep track of them in maps.
            Map<String, Integer> nodeIndices = new HashMap<>();
            int driverIndex = 0;
            for (Map<String, Object> driver : drivers) {
                String nodeName = readString(driver, "cluster_name") + ":" + readString(driver, "node_name");

                Integer nodeIndex = nodeIndices.get(nodeName);
                if (nodeIndex == null) {
                    // New node encountered
                    nodeIndex = nodeIndices.size();
                    nodeIndices.put(nodeName, nodeIndex);

                    emitMetadataForNode(nodeName, nodeIndex, builder);
                }

                // Represent each driver as a separate thread, but group them together into one process per node.
                parseDriverProfile(driver, nodeIndex, driverIndex++, builder);
            }
            builder.endArray();

            builder.endObject();

            logger.info("Finished writing to", outputFileName);
        }

        logger.info("Exiting", args[0]);
    }

    /**
     * Emit a metadata event for a new cluster node. We declare the node as a process.
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

    @SuppressWarnings("unchecked")
    /**
     * Uses the legacy Chromium spec for event descriptions:
     * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
     *
     * Associates the driver with a given process and thread id to separate them visually.
     *
     * We'd probably want to upgrade to the newer, more flexible protobuf-based spec by perfetto in the future - but this one also turned
     * out to be tricky to use.
     */
    private static void parseDriverProfile(Map<String, Object> driver, int pid, int tid, XContentBuilder builder) throws IOException {
        String taskDescription = (String) driver.get("task_description");
        String name = taskDescription + " " + pid + ":" + tid;

        emitMetadataForDriver(taskDescription, pid, tid, builder);

        builder.startObject();
        builder.field("ph", "X");
        builder.field("name", name);
        builder.field("cat", taskDescription);
        builder.field("pid", pid);
        builder.field("tid", tid);
        long startMicros = readIntOrLong(driver, "start_millis") * 1000;
        builder.field("ts", startMicros);
        double durationMicros = ((double) readIntOrLong(driver, "took_nanos")) / 1000.0;
        builder.field("dur", durationMicros);
        double cpuDurationMicros = ((double) readIntOrLong(driver, "cpu_nanos")) / 1000.0;
        builder.field("tdur", cpuDurationMicros);

        builder.field("args");
        builder.startObject();
        builder.field("cpu_nanos", readIntOrLong(driver, "cpu_nanos"));
        builder.field("took_nanos", readIntOrLong(driver, "took_nanos"));
        builder.field("iterations", readIntOrLong(driver, "iterations"));
        // TODO: Sleeps have more details
        int sleeps = ((Map<?, ?>) driver.get("sleeps")).size();
        builder.field("sleeps", sleeps);
        builder.field("operators");
        builder.startArray();
        for (Map<String, Object> operator : (List<Map<String, Object>>) driver.get("operators")) {
            builder.value((String) operator.get("operator"));
            // TODO: Add status; needs standardizing the operatur statuses, probably.
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

    private static String readString(Map<String, Object> json, String name) {
        return (String) json.get(name);
    }

    private static Long readIntOrLong(Map<String, Object> json, String name) {
        Object number = json.get(name);

        return number instanceof Long l ? l : ((Integer) number).longValue();
    }
}
