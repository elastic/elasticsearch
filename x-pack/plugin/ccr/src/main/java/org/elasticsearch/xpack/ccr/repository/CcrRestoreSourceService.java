package org.elasticsearch.xpack.ccr.repository;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;
import java.util.Map;

public class CcrRestoreSourceService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(CcrRestoreSourceService.class);

    final private Map<String, Engine.IndexCommitRef> onGoingRestores = ConcurrentCollections.newConcurrentMap();

    public CcrRestoreSourceService(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.closeWhileHandlingException(onGoingRestores.values());
    }

    public void addCommit(String sessionUUID, Engine.IndexCommitRef commit) {
        onGoingRestores.put(sessionUUID, commit);
    }

    public Engine.IndexCommitRef getCommit(String sessionUUID) {
        Engine.IndexCommitRef commit = onGoingRestores.get(sessionUUID);
        if (commit == null) {
            throw new ElasticsearchException("commit for [" + sessionUUID + "] not found");
        }
        return commit;
    }

    public void closeCommit(String sessionUUID) {
        Engine.IndexCommitRef commit = onGoingRestores.remove(sessionUUID);
        if (commit == null) {
            throw new ElasticsearchException("commit for [" + sessionUUID + "] not found");
        }
        IOUtils.closeWhileHandlingException(commit);
    }
}
