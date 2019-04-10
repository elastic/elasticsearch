package org.elasticsearch.gradle.info;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.resources.TextResource;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;

public class PrintGlobalBuildInfoTask extends DefaultTask {

    private RegularFileProperty buildInfoFile;

    @Inject
    public PrintGlobalBuildInfoTask(ObjectFactory objectFactory) {
        this.buildInfoFile = objectFactory.fileProperty();
    }

    @TaskAction
    public void print() {
        getLogger().quiet("=======================================");
        getLogger().quiet("Elasticsearch Build Hamster says Hello!");
        getLogger().quiet(getBuildInfoText().asString());
        getLogger().quiet("  Random Testing Seed   : " + getProject().property("testSeed"));
        getLogger().quiet("=======================================");
    }

    @InputFile
    public RegularFileProperty getBuildInfoFile() {
        return buildInfoFile;
    }

    private TextResource getBuildInfoText() {
        return getProject().getResources().getText().fromFile(buildInfoFile.getAsFile().get());
    }

}
