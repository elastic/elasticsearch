package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.test.rest.ESRestTestCase;

public abstract class AbstractFullClusterRestartTestCase extends ESRestTestCase {

    private final boolean runningAgainstOldCluster = Booleans.parseBoolean(System.getProperty("tests.is_old_cluster"));

    public final boolean isRunningAgainstOldCluster() {
        return runningAgainstOldCluster;
    }

    private final Version oldClusterVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    public final Version getOldClusterVersion() {
        return oldClusterVersion;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

}
