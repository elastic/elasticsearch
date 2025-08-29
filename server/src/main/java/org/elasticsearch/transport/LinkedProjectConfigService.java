/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;

import java.util.Collection;
import java.util.Collections;

public interface LinkedProjectConfigService {

    interface LinkedProjectConfigListener {
        void updateLinkedProject(LinkedProjectConfig config);

        default void skipUnavailableChanged(
            ProjectId originProjectId,
            ProjectId linkedProjectId,
            String linkedProjectAlias,
            boolean skipUnavailable
        ) {}
    }

    void register(LinkedProjectConfigListener listener);

    Collection<LinkedProjectConfig> loadAllLinkedProjectConfigs();

    LinkedProjectConfigService NOOP = new LinkedProjectConfigService() {
        @Override
        public void register(LinkedProjectConfigListener listener) {}

        @Override
        public Collection<LinkedProjectConfig> loadAllLinkedProjectConfigs() {
            return Collections.emptyList();
        }
    };
}
