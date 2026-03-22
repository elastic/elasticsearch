/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport

import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.TaskOutcome

class UpdateTransportVersionsCSVFuncTest extends AbstractTransportVersionFuncTest {
    def runUpdateTask(String... additionalArgs) {
        List<String> args = new ArrayList<>()
        args.add(":myserver:updateTransportVersionsCSV")
        args.addAll(additionalArgs);
        return gradleRunner(args.toArray())
    }

    void assertUpdateSuccess(BuildResult result) {
        assert result.task(":myserver:updateTransportVersionsCSV").outcome == TaskOutcome.SUCCESS
    }

    void assertUpdateFailure(BuildResult result, String expectedOutput) {
        assert result.task(":myserver:updateTransportVersionsCSV").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, expectedOutput)
    }

    void assertTransportVersionsCsv(String expectedContent) {
        File csvFile = file("myserver/src/main/resources/org/elasticsearch/TransportVersions.csv")
        assert csvFile.exists()
        // Normalize both sides: strip leading/trailing whitespace from each line and the whole string
        def actualLines = csvFile.text.readLines().collect { it.strip() }.findAll { it != "" }
        def expectedLines = expectedContent.readLines().collect { it.strip() }.findAll { it != "" }
        assert actualLines == expectedLines
    }

    def "updates csv with minor version"() {
        when:
        def result = runUpdateTask("--stack-version", "9.2.0").build()

        then:
        assertUpdateSuccess(result)
        assertTransportVersionsCsv("""
            9.0.0,8000000
            9.1.0,8012001
            9.2.0,8123000
        """)
    }

    def "updates csv with patch version"() {
        given:
        execute("git checkout main")
        unreferableTransportVersion("initial_9.1.1", "8012002")
        transportVersionUpperBound("9.1", "initial_9.1.1", "8012002")
        execute('git commit -a -m "update"')

        when:
        def result = runUpdateTask("--stack-version", "9.1.1").build()

        then:
        assertUpdateSuccess(result)
        assertTransportVersionsCsv("""
            9.0.0,8000000
            9.1.0,8012001
            9.1.1,8012002
        """)
    }

    def "fails when upper bound does not exist"() {
        when:
        def result = runUpdateTask("--stack-version", "9.3.0").buildAndFail()

        then:
        assertUpdateFailure(result, "Missing upper bound 9.3 for stack version 9.3.0")
    }

    def "is idempotent when version already exists"() {
        when:
        // Run the task twice with the same version
        def result1 = runUpdateTask("--stack-version", "9.2.0").build()
        def result2 = runUpdateTask("--stack-version", "9.2.0").build()

        then:
        assertUpdateSuccess(result1)
        assertUpdateSuccess(result2)
        assertOutputContains(result2.output, "Version 9.2.0 already exists in TransportVersions.csv with correct transport version ID, skipping")
        // Should only have one entry for 9.2.0
        assertTransportVersionsCsv("""
            9.0.0,8000000
            9.1.0,8012001
            9.2.0,8123000
        """)
    }

    def "fails when existing version has wrong transport version ID"() {
        given:
        // Manually add an entry with wrong transport version ID
        javaResource("myserver", "org/elasticsearch/TransportVersions.csv", """
            9.0.0,8000000
            9.1.0,8012001
            9.2.0,9999999
        """)

        when:
        def result = runUpdateTask("--stack-version", "9.2.0").buildAndFail()

        then:
        assertUpdateFailure(result, "Version 9.2.0 already exists in TransportVersions.csv with transport version ID 9999999, but expected 8123000")
    }
}
