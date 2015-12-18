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

import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

import org.gradle.api.DefaultTask
import org.gradle.api.artifacts.UnknownConfigurationException
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.TaskAction

import org.apache.tools.ant.BuildLogger
import org.apache.tools.ant.Project

/**
 * Basic static checking to keep tabs on third party JARs
 */
public class ThirdPartyAuditTask extends DefaultTask {

    // true to be lenient about MISSING CLASSES
    private boolean missingClasses;
    
    // patterns for classes to exclude, because we understand their issues
    private String[] excludes = new String[0];
    
    ThirdPartyAuditTask() {
        dependsOn(project.configurations.testCompile)
        description = "Checks third party JAR bytecode for missing classes, use of internal APIs, and other horrors'"
    }

    /** 
     * Set to true to be lenient with missing classes. By default this check will fail if it finds
     * MISSING CLASSES. This means the set of jars is incomplete. However, in some cases
     * this can be due to intentional exclusions that are well-tested and understood.
     */      
    public void setMissingClasses(boolean value) {
        missingClasses = value;
    }
    
    /**
     * Returns true if leniency about missing classes is enabled.
     */
    public boolean isMissingClasses() {
        return missingClasses;
    }
    
    /**
     * classes that should be excluded from the scan,
     * e.g. because we know what sheisty stuff those particular classes are up to.
     */
    public void setExcludes(String[] classes) {
        for (String s : classes) {
            if (s.indexOf('*') != -1) {
                throw new IllegalArgumentException("illegal third party audit exclusion: '" + s + "', wildcards are not permitted!")
            }
        }
        excludes = classes;
    }
    
    /**
     * Returns current list of exclusions.
     */
    public String[] getExcludes() {
        return excludes;
    }

    @TaskAction
    public void check() {
        AntBuilder ant = new AntBuilder()

        // we are noisy for many reasons, working around performance problems with forbidden-apis, dealing
        // with warnings about missing classes, etc. so we use our own "quiet" AntBuilder
        ant.project.buildListeners.each { listener ->
            if (listener instanceof BuildLogger) {
              listener.messageOutputLevel = Project.MSG_ERR;
            }
        };
        
        // we only want third party dependencies.
        FileCollection jars = project.configurations.testCompile.fileCollection({ dependency -> 
            dependency.group.startsWith("org.elasticsearch") == false
        })
        
        // we don't want provided dependencies, which we have already scanned. e.g. don't
        // scan ES core's dependencies for every single plugin
        try {
            jars -= project.configurations.getByName("provided")
        } catch (UnknownConfigurationException ignored) {}
        
        // no dependencies matched, we are done
        if (jars.isEmpty()) {
            return;
        }
        
        ant.taskdef(name:      "thirdPartyAudit",
                    classname: "de.thetaphi.forbiddenapis.ant.AntTask",
                    classpath: project.configurations.buildTools.asPath)
        
        // print which jars we are going to scan, always
        // this is not the time to try to be succinct! Forbidden will print plenty on its own!
        Set<String> names = new HashSet<>()
        for (File jar : jars) {
            names.add(jar.getName())
        }
        logger.error("[thirdPartyAudit] Scanning: " + names)
        
        // warn that classes are missing
        // TODO: move these to excludes list!
        if (missingClasses) {
            logger.warn("[thirdPartyAudit] WARNING: CLASSES ARE MISSING! Expect NoClassDefFoundError in bug reports from users!")
        }
        
        // TODO: forbidden-apis + zipfileset gives O(n^2) behavior unless we dump to a tmpdir first, 
        // and then remove our temp dir afterwards. don't complain: try it yourself.
        // we don't use gradle temp dir handling, just google it, or try it yourself.
        
        File tmpDir = new File(project.buildDir, 'tmp/thirdPartyAudit')
        
        // clean up any previous mess (if we failed), then unzip everything to one directory
        ant.delete(dir: tmpDir.getAbsolutePath())
        tmpDir.mkdirs()
        for (File jar : jars) {
            ant.unzip(src: jar.getAbsolutePath(), dest: tmpDir.getAbsolutePath())
        }
        
        // convert exclusion class names to binary file names
        String[] excludedFiles = new String[excludes.length];
        for (int i = 0; i < excludes.length; i++) {
            excludedFiles[i] = excludes[i].replace('.', '/') + ".class"
            // check if the excluded file exists, if not, sure sign things are outdated
            if (! new File(tmpDir, excludedFiles[i]).exists()) {
                throw new IllegalStateException("bogus thirdPartyAudit exclusion: '" + excludes[i] + "', not found in any dependency")
            }
        }
        
        // jarHellReprise
        checkSheistyClasses(tmpDir.toPath(), new HashSet<>(Arrays.asList(excludedFiles)));
        
        ant.thirdPartyAudit(internalRuntimeForbidden: true, 
                            failOnUnsupportedJava: false, 
                            failOnMissingClasses: !missingClasses,
                            classpath: project.configurations.testCompile.asPath) {
            fileset(dir: tmpDir, excludes: excludedFiles.join(','))
        }
        // clean up our mess (if we succeed)
        ant.delete(dir: tmpDir.getAbsolutePath())
    }
    
    /**
     * check for sheisty classes: if they also exist in the extensions classloader, its jar hell with the jdk!
     */
    private void checkSheistyClasses(Path root, Set<String> excluded) {
        // system.parent = extensions loader.
        // note: for jigsaw, this evilness will need modifications (e.g. use jrt filesystem!). 
        // but groovy/gradle needs to work at all first!
        ClassLoader ext = ClassLoader.getSystemClassLoader().getParent()
        assert ext != null
        
        Set<String> sheistySet = new TreeSet<>();
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String entry = root.relativize(file).toString()
                if (entry.endsWith(".class")) {
                    if (ext.getResource(entry) != null) {
                        sheistySet.add(entry);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        
        // check if we are ok
        if (sheistySet.isEmpty()) {
            return;
        }
        
        // leniency against exclusions list
        sheistySet.removeAll(excluded);
        
        if (sheistySet.isEmpty()) {
            logger.warn("[thirdPartyAudit] WARNING: JAR HELL WITH JDK! Expect insanely hard-to-debug problems!")
        } else {
            throw new IllegalStateException("JAR HELL WITH JDK! " + sheistySet);
        }
    }
}
