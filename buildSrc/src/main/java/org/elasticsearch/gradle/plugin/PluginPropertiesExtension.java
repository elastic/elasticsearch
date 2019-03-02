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

package org.elasticsearch.gradle.plugin;

import org.gradle.api.Project;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * A container for plugin properties that will be written to the plugin descriptor, for easy
 * manipulation in the gradle DSL.
 */
public class PluginPropertiesExtension {
    private String name;

    private String version;

    private String description;

    private String classname;

    /** Other plugins this plugin extends through SPI */
    private List<String> extendedPlugins = new ArrayList<>();

    private boolean hasNativeController;

    /** Indicates whether the plugin jar should be made available for the transport client. */
    private boolean hasClientJar;

    /** True if the plugin requires the elasticsearch keystore to exist, false otherwise. */
    private boolean requiresKeystore;

    /** A license file that should be included in the built plugin zip. */
    private File licenseFile;

    /**
     * A notice file that should be included in the built plugin zip. This will be
     * extended with notices from the {@code licenses/} directory.
     */
    private File noticeFile;

    private Project project;

    public PluginPropertiesExtension(Project project) {
        Object version = project.getVersion();

        this.name = project.getName();
        this.version = (version != null && version instanceof String)? String.valueOf(version) : null;
        this.project = project;
    }

    @Input
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Input
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Input
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Input
    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    @Input
    public List<String> getExtendedPlugins() {
        return extendedPlugins;
    }

    public void setExtendedPlugins(List<String> extendedPlugins) {
        this.extendedPlugins = extendedPlugins;
    }

    @Input
    public boolean isHasNativeController() {
        return hasNativeController;
    }

    public void setHasNativeController(boolean hasNativeController) {
        this.hasNativeController = hasNativeController;
    }

    @Input
    public boolean isHasClientJar() {
        return hasClientJar;
    }

    public void setHasClientJar(boolean hasClientJar) {
        this.hasClientJar = hasClientJar;
    }

    @Input
    public boolean isRequiresKeystore() {
        return requiresKeystore;
    }

    public void setRequiresKeystore(boolean requiresKeystore) {
        this.requiresKeystore = requiresKeystore;
    }

    @InputFile
    public File getLicenseFile() {
        return licenseFile;
    }

    public void setLicenseFile(File licenseFile) {
        this.project.getExtensions().add("licenseFile", licenseFile);
        this.licenseFile = licenseFile;
    }

    @InputFile
    public File getNoticeFile() {
        return noticeFile;
    }

    public void setNoticeFile(File noticeFile) {
        this.project.getExtensions().add("noticeFile", noticeFile);
        this.noticeFile = noticeFile;
    }

    @Input
    public Project getProject() {
        return project;
    }

    public void setProject(Project project) {
        this.project = project;
    }
}
