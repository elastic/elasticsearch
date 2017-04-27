/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;

import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Extension points for upgrading cluster and index metadata
 */
public class UpgraderPlugin {
    /**
     * Provides a function to modify global custom meta data on startup.
     * <p>
     * Plugins should return the input custom map via {@link UnaryOperator#identity()} if no upgrade is required.
     * @return Never {@code null}. The same or upgraded {@code MetaData.Custom} map.
     * @throws IllegalStateException if the node should not start because at least one {@code MetaData.Custom}
     *         is unsupported
     */
    public UnaryOperator<Map<String, MetaData.Custom>> getCustomMetaDataUpgrader() {
        return UnaryOperator.identity();
    }

    /**
     * Provides a function to modify index template meta data on startup.
     * <p>
     * Plugins should return the input template map via {@link UnaryOperator#identity()} if no upgrade is required.
     * @return Never {@code null}. The same or upgraded {@code IndexTemplateMetaData} map.
     * @throws IllegalStateException if the node should not start because at least one {@code IndexTemplateMetaData}
     *         cannot be upgraded
     */
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return UnaryOperator.identity();
    }

    /**
     * Provides a function to modify index meta data when an index is introduced into the cluster state for the first time.
     * <p>
     * Plugins should return the input index metadata via {@link UnaryOperator#identity()} if no upgrade is required.
     * @return Never {@code null}. The same or upgraded {@code IndexMetaData}.
     * @throws IllegalStateException if the node should not start because the index is unsupported
     */
    public UnaryOperator<IndexMetaData> getIndexMetaDataUpgrader() {
        return UnaryOperator.identity();
    }


}
