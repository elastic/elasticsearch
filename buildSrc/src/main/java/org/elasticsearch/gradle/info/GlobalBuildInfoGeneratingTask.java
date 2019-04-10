package org.elasticsearch.gradle.info;

import org.gradle.api.DefaultTask;
import org.gradle.api.JavaVersion;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;

import java.io.File;
import java.util.Map;

@CacheableTask
public class GlobalBuildInfoGeneratingTask extends DefaultTask {

    private JavaVersion minimumCompilerVersion;
    private JavaVersion minimumRuntimeVersion;
    private File compilerJavaHome;
    private File runtimeJavaHome;
    private Map<Integer, String> javaVersions;
    private File outputFile;

    @Input
    public JavaVersion getMinimumCompilerVersion() {
        return minimumCompilerVersion;
    }

    public void setMinimumCompilerVersion(JavaVersion minimumCompilerVersion) {
        this.minimumCompilerVersion = minimumCompilerVersion;
    }

    @Input
    public JavaVersion getMinimumRuntimeVersion() {
        return minimumRuntimeVersion;
    }

    public void setMinimumRuntimeVersion(JavaVersion minimumRuntimeVersion) {
        this.minimumRuntimeVersion = minimumRuntimeVersion;
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getCompilerJavaHome() {
        return compilerJavaHome;
    }

    public void setCompilerJavaHome(File compilerJavaHome) {
        this.compilerJavaHome = compilerJavaHome;
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getRuntimeJavaHome() {
        return runtimeJavaHome;
    }

    public void setRuntimeJavaHome(File runtimeJavaHome) {
        this.runtimeJavaHome = runtimeJavaHome;
    }

    @Input
    public Map<Integer, String> getJavaVersions() {
        return javaVersions;
    }

    public void setJavaVersions(Map<Integer, String> javaVersions) {
        this.javaVersions = javaVersions;
    }

    @OutputFile
    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }
}
