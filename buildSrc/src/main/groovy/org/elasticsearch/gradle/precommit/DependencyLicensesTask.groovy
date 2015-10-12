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
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.StopActionException
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.VerificationTask

import java.nio.file.Files
import java.security.MessageDigest
import java.util.regex.Matcher
import java.util.regex.Pattern

class DependencyLicensesTask extends DefaultTask {
    static final String SHA_EXTENSION = '.sha1'

    static Task configure(Project project, Closure closure) {
        DependencyLicensesTask task = project.tasks.create(type: DependencyLicensesTask, name: 'dependencyLicenses')
        UpdateShasTask update = project.tasks.create(type: UpdateShasTask, name: 'updateShas')
        update.parentTask = task
        task.configure(closure)
        project.check.dependsOn(task)
        return task
    }

    @InputFiles
    FileCollection dependencies

    @InputDirectory
    File licensesDir = new File(project.projectDir, 'licenses')

    LinkedHashMap<String, String> mappings = new LinkedHashMap<>()

    @Input
    void mapping(Map<String, String> props) {
        String from = props.get('from')
        if (from == null) {
            throw new InvalidUserDataException('Missing "from" setting for license name mapping')
        }
        String to = props.get('to')
        if (to == null) {
            throw new InvalidUserDataException('Missing "to" setting for license name mapping')
        }
        mappings.put(from, to)
    }

    @TaskAction
    void checkDependencies() {
        // TODO: empty license dir (or error when dir exists and no deps)
        if (licensesDir.exists() == false && dependencies.isEmpty() == false) {
            throw new GradleException("Licences dir ${licensesDir} does not exist, but there are dependencies")
        }

        // order is the same for keys and values iteration since we use a linked hashmap
        List<String> mapped = new ArrayList<>(mappings.values())
        Pattern mappingsPattern = Pattern.compile('(' + mappings.keySet().join(')|(') + ')')
        Map<String, Integer> licenses = new HashMap<>()
        Map<String, Integer> notices = new HashMap<>()
        Set<File> shaFiles = new HashSet<File>()

        licensesDir.eachFile {
            String name = it.getName()
            if (name.endsWith(SHA_EXTENSION)) {
                shaFiles.add(it)
            } else if (name.endsWith('-LICENSE') || name.endsWith('-LICENSE.txt')) {
                // TODO: why do we support suffix of LICENSE *and* LICENSE.txt??
                licenses.put(name, 0)
            } else if (name.contains('-NOTICE') || name.contains('-NOTICE.txt')) {
                notices.put(name, 0)
            }
        }

        for (File dependency : dependencies) {
            String jarName = dependency.getName()
            logger.info("Checking license/notice/sha for " + jarName)
            checkSha(dependency, jarName, shaFiles)

            String name = jarName - ~/\-\d+.*/
            Matcher match = mappingsPattern.matcher(name)
            if (match.matches()) {
                int i = 0
                while (i < match.groupCount() && match.group(i + 1) == null) ++i;
                logger.info("Mapped dependency name ${name} to ${mapped.get(i)} for license check")
                name = mapped.get(i)
            }
            checkFile(name, jarName, licenses, 'LICENSE')
            checkFile(name, jarName, notices, 'NOTICE')
        }

        licenses.each { license, count ->
            if (count == 0) {
                throw new GradleException("Unused license ${license}")
            }
        }
        notices.each { notice, count ->
            if (count == 0) {
                throw new GradleException("Unused notice ${notice}")
            }
        }
        if (shaFiles.isEmpty() == false) {
            throw new GradleException("Unused sha files found: \n${shaFiles.join('\n')}")
        }
    }

    void checkSha(File jar, String jarName, Set<File> shaFiles) {
        File shaFile = new File(licensesDir, jarName + SHA_EXTENSION)
        if (shaFile.exists() == false) {
            throw new GradleException("Missing SHA for ${jarName}. Run 'gradle updateSHAs' to create")
        }
        // TODO: shouldn't have to trim, sha files should not have trailing newline
        String expectedSha = shaFile.getText('UTF-8').trim()
        String sha = MessageDigest.getInstance("SHA-1").digest(jar.getBytes()).encodeHex().toString()
        if (expectedSha.equals(sha) == false) {
            throw new GradleException("SHA has changed! Expected ${expectedSha} for ${jarName} but got ${sha}. " +
                                      "\nThis usually indicates a corrupt dependency cache or artifacts changed upstream." +
                                      "\nEither wipe your cache, fix the upstream artifact, or delete ${shaFile} and run updateShas")
        }
        shaFiles.remove(shaFile)
    }

    void checkFile(String name, String jarName, Map<String, Integer> counters, String type) {
        String fileName = "${name}-${type}"
        Integer count = counters.get(fileName)
        if (count == null) {
            // try the other suffix...TODO: get rid of this, just support ending in .txt
            fileName = "${fileName}.txt"
            counters.get(fileName)
        }
        count = counters.get(fileName)
        if (count == null) {
            throw new GradleException("Missing ${type} for ${jarName}, expected in ${fileName}")
        }
        counters.put(fileName, count + 1)
    }

    static class UpdateShasTask extends DefaultTask {
        DependencyLicensesTask parentTask
        @TaskAction
        void updateShas() {
            Set<File> shaFiles = new HashSet<File>()
            parentTask.licensesDir.eachFile {
                String name = it.getName()
                if (name.endsWith(SHA_EXTENSION)) {
                    shaFiles.add(it)
                }
            }
            for (File dependency : parentTask.dependencies) {
                String jarName = dependency.getName()
                File shaFile = new File(parentTask.licensesDir, jarName + SHA_EXTENSION)
                if (shaFile.exists() == false) {
                    logger.lifecycle("Adding sha for ${jarName}")
                    String sha = MessageDigest.getInstance("SHA-1").digest(dependency.getBytes()).encodeHex().toString()
                    shaFile.setText(sha, 'UTF-8')
                } else {
                    shaFiles.remove(shaFile)
                }
            }
            shaFiles.each { shaFile ->
                logger.lifecycle("Removing unused sha ${shaFile.getName()}")
                Files.delete(shaFile.toPath())
            }
        }
    }
}
