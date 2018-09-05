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
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Runs CheckJarHell on a classpath.
 */
public class JarHellTask extends DefaultTask {

    private FileCollection classpath;

    private Object javaHome;

    public JarHellTask() {
        setDescription("Runs CheckJarHell on the configured classpath");
    }

    @TaskAction
    public void runJarHellCheck() {
        LoggedExec.javaexec(getProject(), spec -> {
            spec.classpath(getClasspath());
            spec.executable(getJavaHome() + "/bin/java");
            spec.setMain("org.elasticsearch.bootstrap.JarHell");
        });
        try {
            getSuccessMarker().getParentFile().mkdirs();
            try (FileWriter fw = new FileWriter(getSuccessMarker())) {
                fw.write("");
            }
        } catch (IOException e) {
            throw new GradleException("io exception", e);
        }
    }

    @Input
    public Object getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(Object javaHome) {
        this.javaHome = javaHome;
    }

    @Classpath
    public FileCollection getClasspath() {
        return classpath.filter(file -> file.exists());
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(getProject().getBuildDir(), "markers/" + this.getName());
    }

}
