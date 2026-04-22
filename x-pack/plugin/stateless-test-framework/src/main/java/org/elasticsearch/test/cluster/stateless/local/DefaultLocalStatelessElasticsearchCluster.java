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

package org.elasticsearch.test.cluster.stateless.local;

import org.elasticsearch.test.cluster.local.DefaultLocalElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;

import java.util.function.Supplier;

public class DefaultLocalStatelessElasticsearchCluster extends DefaultLocalElasticsearchCluster<
    LocalClusterSpec,
    StatelessLocalClusterHandle> implements StatelessElasticsearchCluster {

    public DefaultLocalStatelessElasticsearchCluster(Supplier<LocalClusterSpec> specProvider, StatelessLocalClusterFactory clusterFactory) {
        super(specProvider, clusterFactory);
    }

    @Override
    public void upgradeNodeToVersion(int index, Version version, boolean forciblyDestroyOldNode) {
        getHandle().upgradeNodeToVersion(index, version, forciblyDestroyOldNode);
    }

    @Override
    public void restartNodeInPlace(int index, boolean forciblyDestroyOldNode) {
        getHandle().restartNodeInPlace(index, forciblyDestroyOldNode);
    }
}
