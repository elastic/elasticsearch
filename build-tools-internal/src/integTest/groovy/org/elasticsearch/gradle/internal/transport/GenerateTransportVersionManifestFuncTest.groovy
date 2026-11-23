/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport

import org.gradle.testkit.runner.TaskOutcome

class GenerateTransportVersionManifestFuncTest extends AbstractTransportVersionFuncTest {

    /**
     * The manifest ends up in the server jar, which is a cache input of downstream tasks (e.g. BWC tests consuming
     * a distribution). It must therefore not depend on the filesystem's directory iteration order.
     */
    def "manifest lists definitions in sorted order"() {
        given:
        // create definitions in reverse name order so that creation or hash order is unlikely to match sorted order
        (40..1).each { i ->
            referableTransportVersion(String.format("def_%02d", i), "${8200000 + i * 1000}")
            unreferableTransportVersion(String.format("unref_%02d", i), "${8300000 + i * 1000}")
        }

        when:
        def result = gradleRunner(":myserver:generateTransportVersionManifest").build()

        then:
        result.task(":myserver:generateTransportVersionManifest").outcome == TaskOutcome.SUCCESS
        def lines = file("myserver/build/generated-resources/manifest.txt").readLines()
        lines.size() == 86
        lines.contains("referable/def_01.csv")
        lines.contains("unreferable/initial_9.0.0.csv")
        lines == lines.toSorted()
    }
}
