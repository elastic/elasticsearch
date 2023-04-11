/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.coverage;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

@CacheableTask
public abstract class TransportMethodCoverageVerifierTask extends DefaultTask {
    private static final String MISSING_TRANSPORT_TESTS_FILE = "transport-tests/missing-transport-tests.txt";
    public static final String JACOCO_XML_REPORT = "reports/jacoco/testCodeCoverageReport/testCodeCoverageReport.xml";
    public static final double MINIMUM_COVERAGE = 0.9;

    private final XmlReportMethodCoverageVerifier verifier;

    @Inject
    public TransportMethodCoverageVerifierTask(ProjectLayout projectLayout) {
        setDescription("Runs custom method coverage on jacoco report");
        getJacocoXmlReport().convention(projectLayout.getBuildDirectory().file(JACOCO_XML_REPORT));
        Set<String> classesToSkip = loadClassesToSkip();
        this.verifier = new XmlReportMethodCoverageVerifier(classesToSkip, MINIMUM_COVERAGE);
    }

    private static Set<String> loadClassesToSkip() {
        var inputStream = TransportMethodCoverageVerifierTask.class.getResourceAsStream("/" + MISSING_TRANSPORT_TESTS_FILE);
        var reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader.lines()
            .filter(l -> l.startsWith("//") == false)
            .filter(l -> l.trim().equals("") == false)
            .collect(Collectors.toSet());
    }

    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    @SkipWhenEmpty
    public abstract RegularFileProperty getJacocoXmlReport();

    @TaskAction
    public void check() throws IOException {
        try {
            Path path = getJacocoXmlReport().get().getAsFile().toPath();
            byte[] bytes = Files.readAllBytes(path);

            List<XmlReportMethodCoverageVerifier.Failure> failures = verifier.verifyReport(bytes);
            if (failures.size() > 0) {
                throw new GradleException(formatFailures(failures));
            }
        } catch (IOException e) {
            throw new GradleException("Unable to read a report", e);
        }
    }

    private String formatFailures(List<XmlReportMethodCoverageVerifier.Failure> failures) {
        StringBuilder sb = new StringBuilder();
        sb.append("Transport protocol code, using org.elasticsearch.common.io.stream.StreamInput/Output needs more test coverage.");
        sb.append(System.lineSeparator());
        sb.append("Minimum coverage is " + MINIMUM_COVERAGE);
        sb.append(System.lineSeparator());
        String failuresDescription = failures.stream()
            .map(failure -> "Class " + failure.className() + " coverage " + failure.coverage())
            .collect(Collectors.joining(System.lineSeparator()));
        sb.append(failuresDescription);
        return sb.toString();
    }
}
