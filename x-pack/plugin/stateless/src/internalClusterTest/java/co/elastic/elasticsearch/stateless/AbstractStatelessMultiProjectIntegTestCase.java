/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.serverless.multiproject.ServerlessMultiProjectPlugin;
import co.elastic.elasticsearch.serverless.multiproject.action.DeleteProjectAction;
import co.elastic.elasticsearch.serverless.multiproject.action.PutProjectAction;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;

@FixForMultiProject(description = "use file-based settings once ES-11454 and ES-10233 are done")
public abstract class AbstractStatelessMultiProjectIntegTestCase extends AbstractStatelessIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ServerlessMultiProjectPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true);
    }

    protected void createProject(ProjectId projectId) {
        assertTrue(
            internalCluster().client()
                .execute(PutProjectAction.INSTANCE, new PutProjectAction.Request(TimeValue.MINUS_ONE, TimeValue.MINUS_ONE, projectId))
                .actionGet(TimeValue.timeValueSeconds(10))
                .isAcknowledged()
        );
    }

    protected void deleteProject(ProjectId projectId) {
        assertTrue(
            internalCluster().client()
                .execute(DeleteProjectAction.INSTANCE, new DeleteProjectAction.Request(TimeValue.MINUS_ONE, TimeValue.MINUS_ONE, projectId))
                .actionGet(TimeValue.timeValueSeconds(10))
                .isAcknowledged()
        );
    }
}
