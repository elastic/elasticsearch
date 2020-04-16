package org.elasticsearch.gradle.docker;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkerExecutor;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;

public class DockerBuildTask extends DefaultTask {
    private final WorkerExecutor workerExecutor;
    private final RegularFileProperty markerFile = getProject().getObjects().fileProperty();
    private final DirectoryProperty dockerContext = getProject().getObjects().directoryProperty();

    private String[] tags;
    private boolean pull = true;
    private boolean noCache = true;

    @Inject
    public DockerBuildTask(WorkerExecutor workerExecutor) {
        this.workerExecutor = workerExecutor;
        this.markerFile.set(getProject().getLayout().getBuildDirectory().file("markers/" + this.getName() + ".marker"));
    }

    @TaskAction
    public void build() {
        workerExecutor.noIsolation().submit(DockerBuildAction.class, params -> {
            params.getDockerContext().set(dockerContext);
            params.getMarkerFile().set(markerFile);
            params.getTags().set(Arrays.asList(tags));
            params.getPull().set(pull);
            params.getNoCache().set(noCache);
        });
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public DirectoryProperty getDockerContext() {
        return dockerContext;
    }

    @Input
    public String[] getTags() {
        return tags;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }

    @Input
    public boolean isPull() {
        return pull;
    }

    public void setPull(boolean pull) {
        this.pull = pull;
    }

    @Input
    public boolean isNoCache() {
        return noCache;
    }

    public void setNoCache(boolean noCache) {
        this.noCache = noCache;
    }

    @OutputFile
    public RegularFileProperty getMarkerFile() {
        return markerFile;
    }

    public abstract static class DockerBuildAction implements WorkAction<Parameters> {
        private final ExecOperations execOperations;

        @Inject
        public DockerBuildAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        @Override
        public void execute() {
            LoggedExec.exec(execOperations, spec -> {
                spec.executable("docker");

                spec.args("build", getParameters().getDockerContext().get().getAsFile().getAbsolutePath());

                if (getParameters().getPull().get()) {
                    spec.args("--pull");
                }

                if (getParameters().getNoCache().get()) {
                    spec.args("--no-cache");
                }

                getParameters().getTags().get().forEach(tag -> spec.args("--tag", tag));
            });

            try {
                getParameters().getMarkerFile().getAsFile().get().createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("Failed to create marker file", e);
            }
        }
    }

    interface Parameters extends WorkParameters {
        DirectoryProperty getDockerContext();

        RegularFileProperty getMarkerFile();

        ListProperty<String> getTags();

        Property<Boolean> getPull();

        Property<Boolean> getNoCache();
    }
}
