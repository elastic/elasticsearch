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

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.TestStatelessCommitService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.PluginComponentBinding;

import java.util.Collection;

public class TestStateless extends Stateless {

    static final Logger logger = LogManager.getLogger(TestStateless.class);

    public TestStateless(Settings settings) {
        super(settings);
    }

    @Override
    public Collection<Object> createComponents(PluginServices services) {
        final Collection<Object> components = super.createComponents(services);
        components.add(
            new PluginComponentBinding<>(
                StatelessCommitService.class,
                components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
            )
        );
        return components;
    }

    @Override
    protected StatelessCommitService createStatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        Client client,
        StatelessCommitCleaner commitCleaner,
        SharedBlobCacheWarmingService cacheWarmingService
    ) {
        return new TestStatelessCommitService(settings, objectStoreService, clusterService, client, commitCleaner, cacheWarmingService);
    }
}
