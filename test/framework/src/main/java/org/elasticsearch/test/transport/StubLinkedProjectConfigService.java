/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.transport;

import org.elasticsearch.transport.LinkedProjectConfig;
import org.elasticsearch.transport.LinkedProjectConfigService;

import java.util.Collection;
import java.util.Collections;

/**
 * A no-op stub implementation of {@link LinkedProjectConfigService} intended for use in test scenarios where linked project
 * configuration updates are not needed.
 */
public class StubLinkedProjectConfigService implements LinkedProjectConfigService {

    public static final StubLinkedProjectConfigService INSTANCE = new StubLinkedProjectConfigService();

    @Override
    public void register(LinkedProjectConfigListener listener) {}

    @Override
    public Collection<LinkedProjectConfig> getInitialLinkedProjectConfigs() {
        return Collections.emptyList();
    }
}
