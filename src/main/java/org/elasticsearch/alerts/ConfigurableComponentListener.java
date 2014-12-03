/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.common.settings.Settings;

/**
 * This interface allows a component to register itself for configuration updates
 */
public interface ConfigurableComponentListener {
    void receiveConfigurationUpdate(Settings settings);
}
