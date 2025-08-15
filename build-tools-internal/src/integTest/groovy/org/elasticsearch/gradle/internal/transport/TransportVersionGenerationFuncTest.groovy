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

class TransportVersionGenerationFuncTest extends AbstractTransportVersionFuncTest {
    def "test setup works"() {
        when:
        def result = gradleRunner("generateTransportVersionDefinition").build()
        then:
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
    }

    /*
    TODO: Add tests that check that:
        - TVs added ontop of main in git, but are no longer referenced, are deleted
        - name without branches param should fail
        - branches without name param should fail (+ other invalid combos)
        - multiple branches should create patch versions
        - a single branch value should create only a primary id
        - a latest file without a corresponding definition file should be reverted to main
        - a merge conflict should be resolved, resulting in regeneration of the latest file.
        -
     */
}
