/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import org.gradle.api.Project;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.plugins.ExtraPropertiesExtension;

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

    private PluginType type = PluginType.ISOLATED;

    private String javaOpts = "";

    /** Whether a license agreement must be accepted before this plugin can be installed. */
    private boolean isLicensed = false;

    /** True if the plugin requires the elasticsearch keystore to exist, false otherwise. */
    private boolean requiresKeystore;

    /** A license file that should be included in the built plugin zip. */
    private File licenseFile;

    /**
     * A notice file that should be included in the built plugin zip. This will be
     * extended with notices from the {@code licenses/} directory.
     */
    private File noticeFile;

    private final Project project;
    private CopySpec bundleSpec;

    public PluginPropertiesExtension(Project project) {
        this.project = project;
    }

    public String getName() {
        return name == null ? project.getName() : name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version == null ? project.getVersion().toString() : version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    public List<String> getExtendedPlugins() {
        return this.extendedPlugins;
    }

    public boolean isHasNativeController() {
        return hasNativeController;
    }

    public void setHasNativeController(boolean hasNativeController) {
        this.hasNativeController = hasNativeController;
    }

    public PluginType getType() {
        return type;
    }

    public void setType(PluginType type) {
        this.type = type;
    }

    public String getJavaOpts() {
        return javaOpts;
    }

    public void setJavaOpts(String javaOpts) {
        this.javaOpts = javaOpts;
    }

    public boolean isLicensed() {
        return isLicensed;
    }

    public void setLicensed(boolean licensed) {
        isLicensed = licensed;
    }

    public boolean isRequiresKeystore() {
        return requiresKeystore;
    }

    public void setRequiresKeystore(boolean requiresKeystore) {
        this.requiresKeystore = requiresKeystore;
    }

    public File getLicenseFile() {
        return licenseFile;
    }

    public void setLicenseFile(File licenseFile) {
        ExtraPropertiesExtension extraProperties = this.project.getExtensions().getExtraProperties();
        RegularFileProperty regularFileProperty = extraProperties.has("licenseFile")
            ? (RegularFileProperty) extraProperties.get("licenseFile")
            : project.getObjects().fileProperty();
        regularFileProperty.set(licenseFile);
        this.licenseFile = licenseFile;
    }

    public File getNoticeFile() {
        return noticeFile;
    }

    public void setNoticeFile(File noticeFile) {
        ExtraPropertiesExtension extraProperties = this.project.getExtensions().getExtraProperties();
        RegularFileProperty regularFileProperty = extraProperties.has("noticeFile")
            ? (RegularFileProperty) extraProperties.get("noticeFile")
            : project.getObjects().fileProperty();
        regularFileProperty.set(noticeFile);
        this.noticeFile = noticeFile;
    }

    public Project getProject() {
        return project;
    }

    public void setExtendedPlugins(List<String> extendedPlugins) {
        this.extendedPlugins = extendedPlugins;
    }

    public void setBundleSpec(CopySpec bundleSpec) {
        this.bundleSpec = bundleSpec;
    }

    public CopySpec getBundleSpec() {
        return bundleSpec;
    }
}
