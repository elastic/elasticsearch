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

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.TaskAction;

import java.io.File;

/**
 * Runs CheckJarHell on a classpath.
 */
@CacheableTask
public class JarHellTask extends PrecommitTask {

    private FileCollection classpath;

    public JarHellTask() {
        setDescription("Runs CheckJarHell on the configured classpath");
    }

    @TaskAction
    public void runJarHellCheck() {
        LoggedExec.javaexec(getProject(), spec -> {
            spec.classpath(getClasspath());
            spec.setMain("org.elasticsearch.bootstrap.JarHell");
        });
    }

    // We use compile classpath normalization here because class implementation changes are irrelevant for the purposes of jar hell.
    // We only care about the runtime classpath ABI here.
    @CompileClasspath
    public FileCollection getClasspath() {
        return classpath.filter(File::exists);
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

}
