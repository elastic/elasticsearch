package org.elasticsearch.gradle.tasks;

import org.elasticsearch.gradle.testclusters.RunTask;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.options.Option;

public class Run extends DefaultTask {

    @Internal
    private boolean debug = false;

    @Option(option = "debug-jvm", description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch.")
    public void setDebug(boolean enabled) {
        getDistributionRunTask().configure(runTask -> runTask.setDebug(enabled));
    }

    @Option(option = "data-dir", description = "Override the base data directory used by the testcluster")
    public void setDataDir(String dataDirStr) {
        getDistributionRunTask().configure((r) -> r.setDataDir(dataDirStr));
    }

    @Option(option = "keystore-password", description = "Set the elasticsearch keystore password")
    public void setKeystorePassword(String password) {
        getDistributionRunTask().configure(runTask -> runTask.setKeystorePassword(password));
    }

    public boolean getDebug() {
        return debug;
    }

    private TaskProvider<RunTask> getDistributionRunTask() {
        return getProject().project(":distribution").getTasks().withType(RunTask.class).named("run");
    }

}
