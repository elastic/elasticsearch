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
    
    @Input
    private String name;
    @Input
    private String version;
    @Input
    private String description;
    @Input
    private String classname;
    /**
     * Other plugins this plugin extends through SPI
     */
    @Input
    private List<String> extendedPlugins = new ArrayList<String>();
    @Input
    private boolean hasNativeController = false;
    /**
     * Indicates whether the plugin jar should be made available for the transport client.
     */
    @Input
    private boolean hasClientJar = false;
    /**
     * True if the plugin requires the elasticsearch keystore to exist, false otherwise.
     */
    @Input
    private boolean requiresKeystore = false;
    /**
     * A license file that should be included in the built plugin zip.
     */
    private File licenseFile = null;
    /**
     * A notice file that should be included in the built plugin zip. This will be
     * extended with notices from the {@code licenses/} directory.
     */
    private File noticeFile = null;
    private Project project = null;

    public PluginPropertiesExtension(Project project) {
        name = project.getName();
        version = project.getVersion().toString();
        this.project = project;
    }

    @InputFile
    public File getLicenseFile() {
        return licenseFile;
    }

    public void setLicenseFile(File licenseFile) {
        //project.ext.licenseFile = licenseFile;
        this.licenseFile = licenseFile;
    }

    @InputFile
    public File getNoticeFile() {
        return noticeFile;
    }

    public void setNoticeFile(File noticeFile) {
        //project.ext.noticeFile = noticeFile;
        this.noticeFile = noticeFile;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
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
        return extendedPlugins;
    }

    public void setExtendedPlugins(List<String> extendedPlugins) {
        this.extendedPlugins = extendedPlugins;
    }

    public boolean getHasNativeController() {
        return hasNativeController;
    }

    public boolean isHasNativeController() {
        return hasNativeController;
    }

    public void setHasNativeController(boolean hasNativeController) {
        this.hasNativeController = hasNativeController;
    }

    public boolean getHasClientJar() {
        return hasClientJar;
    }

    public boolean isHasClientJar() {
        return hasClientJar;
    }

    public void setHasClientJar(boolean hasClientJar) {
        this.hasClientJar = hasClientJar;
    }

    public boolean getRequiresKeystore() {
        return requiresKeystore;
    }

    public boolean isRequiresKeystore() {
        return requiresKeystore;
    }

    public void setRequiresKeystore(boolean requiresKeystore) {
        this.requiresKeystore = requiresKeystore;
    }

    public Project getProject() {
        return project;
    }

    public void setProject(Project project) {
        this.project = project;
    }
}
