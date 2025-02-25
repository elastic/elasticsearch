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
        int driverIndex = 0;
        try (
            OutputStream output = Files.newOutputStream(outputFileName);
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, output)
        ) {
            logger.info("Starting to write {}", outputFileName);

            builder.startObject();
            builder.field("displayTimeUnit", "ns");
            builder.field("systemTraceEvents", "SystemTraceData");

            builder.field("otherData");
            builder.startObject();
            builder.field("version", "My Application v1.0");
            builder.endObject();

            builder.field("stackFrames");
            builder.startObject();
            builder.endObject();

            builder.field("samples");
            builder.startArray();
            builder.endArray();

            builder.field("traceEvents");
            builder.startArray();
            for (Map<String, Object> driver : drivers) {
                parseDriverProfile(driver, driverIndex++, builder);
            }
            builder.endArray();

            builder.endObject();

            logger.info("Finished writing to", outputFileName);
        }

        logger.info("Exiting", args[0]);
    }

    @SuppressWarnings("unchecked")
    /**
     * Uses the legacy Chromium spec for event descriptions:
     * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
     *
     * We probably want to upgrade to the newer, more flexible protobuf-based spec by perfetto in the future.
     */
    private static void parseDriverProfile(Map<String, Object> driver, int driverIndex, XContentBuilder builder) throws IOException {
        String taskDescription = (String) driver.get("task_description");
        String name = taskDescription + driverIndex;

        /// Start event
        builder.startObject();
        builder.field("ph", "B");
        builder.field("name", name);
        builder.field("cat", taskDescription);
        builder.field("pid", 0);
        builder.field("ts", readIntOrLong(driver, "start_millis") * 1000);
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

        /// End event
        builder.startObject();
        builder.field("ph", "E");
        builder.field("name", name);
        builder.field("pid", 0);
        builder.field("ts", readIntOrLong(driver, "stop_millis") * 1000);
        builder.endObject();
    }

    private static Long readIntOrLong(Map<String, Object> json, String name) {
        Object number = json.get(name);

        return number instanceof Long l ? l : ((Integer) number).longValue();
    }
}
