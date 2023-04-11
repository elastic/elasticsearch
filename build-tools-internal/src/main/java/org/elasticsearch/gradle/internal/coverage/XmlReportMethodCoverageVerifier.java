/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.coverage;

import groovy.lang.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class XmlReportMethodCoverageVerifier {
    private static final String STREAM_INPUT_DESC = "Lorg/elasticsearch/common/io/stream/StreamInput;";
    private static final String STREAM_OUTPUT_DESC = "Lorg/elasticsearch/common/io/stream/StreamOutput;";
    private Set<String> classesToSkip;
    private double minimumCoverage;

    public XmlReportMethodCoverageVerifier(Set<String> classesToSkip, double minimumCoverage) {
        this.classesToSkip = classesToSkip;
        this.minimumCoverage = minimumCoverage;
    }

    record Failure(String className, double coverage) {

    }

    public List<Failure> verifyReport(byte[] xmlReport) throws IOException {
        JsonNode root = new XmlMapper().readTree(xmlReport);
        Map<String, Tuple2<Integer, Integer>> classToMissedAndCovered = new HashMap<>();

        JsonNode packages = root.path("package");
        parseJsonNode(packages, p -> parsePackage(p, classToMissedAndCovered));

        return createFailures(classToMissedAndCovered);
    }

    @NotNull
    private List<Failure> createFailures(Map<String, Tuple2<Integer, Integer>> classToMissedAndCovered) {
        List<Failure> failures = new ArrayList<>();

        for (Map.Entry<String, Tuple2<Integer, Integer>> classNameToMissedAndCovered : classToMissedAndCovered.entrySet()) {
            double missed = (double) classNameToMissedAndCovered.getValue().getV1();
            double covered = (double) classNameToMissedAndCovered.getValue().getV2();
            double coverage = covered / (covered + missed);
            if (coverage < minimumCoverage) {
                String className = classNameToMissedAndCovered.getKey();
                if (classesToSkip.contains(className) == false) {
                    failures.add(new Failure(className, coverage));
                }
            }
        }
        return failures;
    }

    private void parsePackage(JsonNode packageElement, Map<String, Tuple2<Integer, Integer>> classToMissedAndCovered) {
        JsonNode classes = packageElement.path("class");
        parseJsonNode(classes, c -> parseClass(c, classToMissedAndCovered));
    }

    private void parseClass(JsonNode clazz, Map<String, Tuple2<Integer, Integer>> classToMissedAndCovered) {
        JsonNode methods = clazz.path("method");
        parseJsonNode(methods, m -> parseMethod(m, clazz, classToMissedAndCovered));
    }

    private void parseMethod(JsonNode method, JsonNode clazz, Map<String, Tuple2<Integer, Integer>> classToMissedAndCovered) {
        if (method.get("desc").textValue().contains(STREAM_INPUT_DESC) || method.get("desc").textValue().contains(STREAM_OUTPUT_DESC)) {
            JsonNode counters = method.path("counter");
            parseJsonNode(counters, c -> parseCounter(c, clazz, classToMissedAndCovered));
        }
    }

    private void parseCounter(JsonNode counter, JsonNode clazz, Map<String, Tuple2<Integer, Integer>> classToMissedAndCovered) {
        if (counter.get("type").textValue().equals("INSTRUCTION")) {

            String className = clazz.get("name").textValue();
            Tuple2<Integer, Integer> missedAndCovered = classToMissedAndCovered.computeIfAbsent(className, c -> new Tuple2<>(0, 0));

            int missed = counter.get("missed").asInt();
            int covered = counter.get("covered").asInt();

            Tuple2<Integer, Integer> missedAndCoveredNew = new Tuple2<>(
                missedAndCovered.getV1() + missed,
                missedAndCovered.getV2() + covered
            );
            classToMissedAndCovered.put(className, missedAndCoveredNew);
        }
    }

    private void parseJsonNode(JsonNode node, Consumer<JsonNode> consumer) {
        if (node.isArray()) {
            node.forEach(consumer);
        } else if (node.isObject()) {
            consumer.accept(node);
        }
    }
}
