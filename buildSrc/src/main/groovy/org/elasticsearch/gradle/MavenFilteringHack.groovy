/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle

import org.apache.tools.ant.filters.ReplaceTokens
import org.gradle.api.file.CopySpec

/**
 * Gradle provides "expansion" functionality using groovy's SimpleTemplatingEngine (TODO: check name).
 * However, it allows substitutions of the form {@code $foo} (no curlies). Rest tests provide
 * some substitution from the test runner, which this form is used for.
 *
 * This class provides a helper to do maven filtering, where only the form {@code $\{foo\}} is supported.
 *
 * TODO: we should get rid of this hack, and make the rest tests use some other identifier
 * for builtin vars
 */
class MavenFilteringHack {
    /**
     * Adds a filter to the given copy spec that will substitute maven variables.
     * @param CopySpec
     */
    static void filter(CopySpec copySpec, Map substitutions) {
        Map mavenSubstitutions = substitutions.collectEntries() {
            key, value -> ["{${key}".toString(), value.toString()]
        }
        copySpec.filter(ReplaceTokens, tokens: mavenSubstitutions, beginToken: '$', endToken: '}')
    }
}
