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
package org.elasticsearch.gradle.precommit;

import org.gradle.api.file.FileCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ThirdPartyAuditNew extends ForbiddenApisCliTask {

    static final Pattern MISSING_CLASS_PATTERN = Pattern.compile(
        "WARNING: The referenced class '(.*)' cannot be loaded\\. Please fix the classpath\\!"
    );

    static final Pattern VIOLATION_PATTERN = Pattern.compile(
        "\\s\\sin ([a-zA-Z0-9\\$\\.]+) \\(.*\\)"
    );

    // patterns for classes to exclude, because we understand their issues
    private List<String> excludes = new ArrayList<>();

    /**
     * Input for the task. Set javadoc for {#link getJars} for more. Protected
     * so the afterEvaluate closure in the constructor can write it.
     */
    protected FileCollection jars;

    /**
     * Classpath against which to run the third patty audit. Protected so the
     * afterEvaluate closure in the constructor can write it.
     */
    protected FileCollection classpath;


}
