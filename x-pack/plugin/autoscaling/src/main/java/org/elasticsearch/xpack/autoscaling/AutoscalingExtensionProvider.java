/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.plugins.AbstractPluginExtensionProvider;

/** Service provider for autoscaling extensions. */
public abstract class AutoscalingExtensionProvider extends AbstractPluginExtensionProvider {

    public AutoscalingExtensionProvider(Class<?> extensionType, Class<?> argType) {
        super(extensionType, argType);
    }
}
