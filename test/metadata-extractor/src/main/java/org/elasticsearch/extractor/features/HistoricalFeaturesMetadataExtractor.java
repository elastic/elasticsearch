/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.extractor.features;

import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.logging.LogConfigurator;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

public class HistoricalFeaturesMetadataExtractor {
    private final ClassLoader classLoader;

    static {
        // Make sure we initialize logging since this is normally done by Elasticsearch startup
        LogConfigurator.configureESLogging();
    }

    public HistoricalFeaturesMetadataExtractor(ClassLoader classLoader) {
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

        new HistoricalFeaturesMetadataExtractor(HistoricalFeaturesMetadataExtractor.class.getClassLoader()).generateMetadataFile(
            outputFile
        );
    }

    public void generateMetadataFile(Path outputFile) {
        try (
            OutputStream os = Files.newOutputStream(outputFile, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
            XContentGenerator generator = JsonXContent.jsonXContent.createGenerator(os)
        ) {
            generator.writeStartObject();
            extractHistoricalFeatureMetadata((historical, names) -> {
                generator.writeFieldName("historical_features");
                generator.writeStartObject();
                for (Map.Entry<NodeFeature, Version> entry : historical.entrySet()) {
                    generator.writeStringField(entry.getKey().id(), entry.getValue().toString());
                }
                generator.writeEndObject();
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

    void extractHistoricalFeatureMetadata(CheckedBiConsumer<Map<NodeFeature, Version>, Set<String>, IOException> metadataConsumer)
        throws IOException {
        Map<NodeFeature, Version> historicalFeatures = new HashMap<>();
        Set<String> featureNames = new HashSet<>();
        ServiceLoader<FeatureSpecification> featureSpecLoader = ServiceLoader.load(FeatureSpecification.class, classLoader);
        for (FeatureSpecification featureSpecification : featureSpecLoader) {
            historicalFeatures.putAll(featureSpecification.getHistoricalFeatures());
            featureSpecification.getFeatures().stream().map(NodeFeature::id).forEach(featureNames::add);
        }
        metadataConsumer.accept(historicalFeatures, featureNames);
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: HistoricalFeaturesMetadataExtractor <output file>");
        System.exit(1);
    }
}
