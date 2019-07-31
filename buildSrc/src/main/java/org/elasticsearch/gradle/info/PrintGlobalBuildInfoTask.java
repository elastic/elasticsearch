package org.elasticsearch.gradle.info;

import org.gradle.api.DefaultTask;
import org.gradle.api.JavaVersion;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.resources.TextResource;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class PrintGlobalBuildInfoTask extends DefaultTask {
    private final RegularFileProperty buildInfoFile;
    private final RegularFileProperty compilerVersionFile;
    private final RegularFileProperty runtimeVersionFile;
    private final RegularFileProperty fipsJvmFile;
    private List<Runnable> globalInfoListeners = new ArrayList<>();

    @Inject
    public PrintGlobalBuildInfoTask(ObjectFactory objectFactory) {
        this.buildInfoFile = objectFactory.fileProperty();
        this.compilerVersionFile = objectFactory.fileProperty();
        this.runtimeVersionFile = objectFactory.fileProperty();
        this.fipsJvmFile = objectFactory.fileProperty();
    }

    @InputFile
    public RegularFileProperty getBuildInfoFile() {
        return buildInfoFile;
    }

    @InputFile
    public RegularFileProperty getCompilerVersionFile() {
        return compilerVersionFile;
    }

    @InputFile
    public RegularFileProperty getRuntimeVersionFile() {
        return runtimeVersionFile;
    }

    @InputFile
    public RegularFileProperty getFipsJvmFile() {
        return fipsJvmFile;
    }

    public void setGlobalInfoListeners(List<Runnable> globalInfoListeners) {
        this.globalInfoListeners = globalInfoListeners;
    }

    @TaskAction
    public void print() {
        getLogger().quiet("=======================================");
        getLogger().quiet("Elasticsearch Build Hamster says Hello!");
        getLogger().quiet(getFileText(getBuildInfoFile()).asString());
        getLogger().quiet("  Random Testing Seed   : " + getProject().property("testSeed"));
        getLogger().quiet("=======================================");

        setGlobalProperties();
        globalInfoListeners.forEach(Runnable::run);

        // Since all tasks depend on this task, and it always runs for every build, this makes sure that lifecycle tasks will still
        // correctly report as UP-TO-DATE, since the convention is a lifecycle task (i.e. assemble, build, etc) will only be marked as
        // UP-TO-DATE if all upstream tasks were also UP-TO-DATE.
        setDidWork(false);
    }

    private TextResource getFileText(RegularFileProperty regularFileProperty) {
        return getProject().getResources().getText().fromFile(regularFileProperty.getAsFile().get());
    }

    private void setGlobalProperties() {
        getProject().getRootProject().allprojects(p -> {
            ExtraPropertiesExtension ext = p.getExtensions().getByType(ExtraPropertiesExtension.class);
            ext.set("compilerJavaVersion", JavaVersion.valueOf(getFileText(getCompilerVersionFile()).asString()));
            ext.set("runtimeJavaVersion", JavaVersion.valueOf(getFileText(getRuntimeVersionFile()).asString()));
            ext.set("inFipsJvm", Boolean.valueOf(getFileText(getFipsJvmFile()).asString()));
        });
    }
}
