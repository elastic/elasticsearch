/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.extractor.features;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Stream;

public class ClusterFeaturesMetadataExtractor {
    private final ClassLoader classLoader;

    static {
        // Make sure we initialize logging since this is normally done by Elasticsearch startup
        LogConfigurator.configureESLogging();
    }

    public ClusterFeaturesMetadataExtractor(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            printUsageAndExit();
        }

        Path outputFile = null;
        try {
            outputFile = Paths.get(args[0]);
        } catch (InvalidPathException e) {
            printUsageAndExit();
        }

        new ClusterFeaturesMetadataExtractor(ClusterFeaturesMetadataExtractor.class.getClassLoader()).generateMetadataFile(outputFile);
    }

    public void generateMetadataFile(Path outputFile) {
        try (
            OutputStream os = Files.newOutputStream(outputFile, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
            XContentGenerator generator = JsonXContent.jsonXContent.createGenerator(os)
        ) {
            generator.writeStartObject();
            extractClusterFeaturesMetadata(names -> {
                generator.writeFieldName("feature_names");
                generator.writeStartArray();
                for (var entry : names) {
                    generator.writeString(entry);
                }
                generator.writeEndArray();
            });
            generator.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void extractClusterFeaturesMetadata(CheckedConsumer<Set<String>, IOException> metadataConsumer) throws IOException {
        Set<String> featureNames = new HashSet<>();
        ServiceLoader<FeatureSpecification> featureSpecLoader = ServiceLoader.load(FeatureSpecification.class, classLoader);
        for (FeatureSpecification featureSpecification : featureSpecLoader) {
            Stream.concat(featureSpecification.getFeatures().stream(), featureSpecification.getTestFeatures().stream())
                .map(NodeFeature::id)
                .forEach(featureNames::add);
        }
        metadataConsumer.accept(featureNames);
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: ClusterFeaturesMetadataExtractor <output file>");
        System.exit(1);
    }
}
