package org.elasticsearch.gradle.info;

import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;

import java.io.File;

public class JavaHome {
    private Integer version;
    private File javaHome;

    private JavaHome(int version, File javaHome) {
        this.version = version;
        this.javaHome = javaHome;
    }

    public static JavaHome of(int version, File javaHome) {
        return new JavaHome(version, javaHome);
    }

    @Input
    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    @InputDirectory
    public File getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(File javaHome) {
        this.javaHome = javaHome;
    }
}
