/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.coverage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class XmlReportMethodCoverageVerifier {
    private static final String STREAM_INPUT_DESC = "Lorg/elasticsearch/common/io/stream/StreamInput;";
    private static final String STREAM_OUTPUT_DESC = "Lorg/elasticsearch/common/io/stream/StreamOutput;";
    public static final double MINIMUM_COVERAGE = 0.1;
    private Set<String> classesToSkip;

    public XmlReportMethodCoverageVerifier(Set<String> classesToSkip) {
        this.classesToSkip = classesToSkip;
    }

    record Failure(String className, String methodName, double coverage) {

    }

    public List<Failure> verifyReport(byte[] xmlReport) throws IOException {
        JsonNode root = new XmlMapper().readTree(xmlReport);
        List<Failure> failures = new ArrayList<>();

        JsonNode packages = root.path("package");
        parseJsonNode(packages, p -> parsePackage(p, failures));

        return failures;
    }

    private void parsePackage(JsonNode packageElement, List<Failure> failures) {
        JsonNode classes = packageElement.path("class");
        parseJsonNode(classes, c -> parseClass(c, failures));
    }

    private void parseClass(JsonNode clazz, List<Failure> failures) {
        JsonNode methods = clazz.path("method");
        parseJsonNode(methods, m -> parseMethod(m, clazz, failures));
    }

    private void parseMethod(JsonNode method, JsonNode clazz, List<Failure> failures) {
        if (method.get("desc").textValue().contains(STREAM_INPUT_DESC) || method.get("desc").textValue().contains(STREAM_OUTPUT_DESC)) {
            JsonNode counters = method.path("counter");
            parseJsonNode(counters, c -> parseCounter(c, method, clazz, failures));
        }
    }

    private void parseCounter(JsonNode counter, JsonNode method, JsonNode clazz, List<Failure> failures) {
        if (counter.get("type").textValue().equals("INSTRUCTION")) {
            double missed = counter.get("missed").asDouble();
            double covered = counter.get("covered").asDouble();
            double coverage = covered / (covered + missed);
            if (coverage < MINIMUM_COVERAGE) {
                String className = clazz.get("name").textValue();
                String methodName = method.get("name").textValue();
                if (classesToSkip.contains(className) == false) {
                    failures.add(new Failure(className, methodName, coverage));
                }
            }
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
