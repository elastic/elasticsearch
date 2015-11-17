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
package org.elasticsearch.gradle.precommit

import org.gradle.api.DefaultTask
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.OutputFiles
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.util.PatternFilterable
import org.gradle.api.tasks.util.PatternSet

import java.util.regex.Pattern

/**
 * Checks for patterns in source files for the project which are forbidden.
 */
class ForbiddenPatternsTask extends DefaultTask {
    Map<String,String> patterns = new LinkedHashMap<>()
    PatternFilterable filesFilter = new PatternSet()

    @OutputFile
    File outputMarker = new File(project.buildDir, "markers/forbiddenPatterns")

    ForbiddenPatternsTask() {
        // we always include all source files, and exclude what should not be checked
        filesFilter.include('**')
        // exclude known binary extensions
        filesFilter.exclude('**/*.gz')
        filesFilter.exclude('**/*.ico')
        filesFilter.exclude('**/*.jar')
        filesFilter.exclude('**/*.zip')
        filesFilter.exclude('**/*.jks')
        filesFilter.exclude('**/*.crt')
        filesFilter.exclude('**/*.png')

        // TODO: add compile and test compile outputs as this tasks outputs, so we don't rerun when source files haven't changed
    }

    /** Adds a file glob pattern to be excluded */
    void exclude(String... excludes) {
        this.filesFilter.exclude(excludes)
    }

    /** Adds pattern to forbid */
    void rule(Map<String,String> props) {
        String name = props.get('name')
        if (name == null) {
            throw new IllegalArgumentException('Missing [name] for invalid pattern rule')
        }
        String pattern = props.get('pattern')
        if (pattern == null) {
            throw new IllegalArgumentException('Missing [pattern] for invalid pattern rule')
        }
        // TODO: fail if pattern contains a newline, it won't work (currently)
        patterns.put(name, pattern)
    }

    /** Returns the files this task will check */
    @InputFiles
    FileCollection files() {
        List<FileCollection> collections = new ArrayList<>()
        for (SourceSet sourceSet : project.sourceSets) {
            collections.add(sourceSet.allSource.matching(filesFilter))
        }
        return project.files(collections.toArray())
    }

    @TaskAction
    void checkInvalidPatterns() {
        Pattern allPatterns = Pattern.compile('(' + patterns.values().join(')|(') + ')')
        List<String> failures = new ArrayList<>()
        for (File f : files()) {
            f.eachLine('UTF-8') { line, lineNumber ->
                if (allPatterns.matcher(line).find()) {
                    addErrorMessages(failures, f, (String)line, (int)lineNumber)
                }
            }
        }
        if (failures.isEmpty() == false) {
            throw new IllegalArgumentException('Found invalid patterns:\n' + failures.join('\n'))
        }
        outputMarker.setText('done', 'UTF-8')
    }

    // iterate through patterns to find the right ones for nice error messages
    void addErrorMessages(List<String> failures, File f, String line, int lineNumber) {
        String path = project.getRootProject().projectDir.toURI().relativize(f.toURI()).toString()
        for (Map.Entry<String,String> pattern : patterns.entrySet()) {
            if (Pattern.compile(pattern.value).matcher(line).find()) {
                failures.add('- ' + pattern.key + ' on line ' + lineNumber + ' of ' + path)
            }
        }
    }
}
