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

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

/**
 * Factory for creating {@link RefreshManagerService} instances.
 * The exact implementation is provided via SPI - otherwise, a default no-op implementation is used.
 */
public interface RefreshManagerServiceFactory {

    RefreshManagerService create(Settings settings, ClusterService clusterService);
}
