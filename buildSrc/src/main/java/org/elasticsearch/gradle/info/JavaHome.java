package org.elasticsearch.gradle.info;

import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;

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

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getJavaHome() {
        return javaHome;
    }
}
