/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.util.resource.Resource;

import java.util.function.Function;

public interface PluginInstallSpec {

    /**
     * Override bundled plugin properties file with the given {@link Resource}. The provided override function receives the original
     * file content as function argument.
     *
     * @param override function returning resource used to override bundled properties file
     */
    PluginInstallSpec withPropertiesOverride(Function<? super String, ? extends Resource> override);

    /**
     * Override bundled entitlements policy file with the given {@link Resource}. The provided override function receives the original
     * file content as function argument.
     *
     * @param override function returning resource used to override bundled entitlements policy file
     */
    PluginInstallSpec withEntitlementsOverride(Function<? super String, ? extends Resource> override);
}
