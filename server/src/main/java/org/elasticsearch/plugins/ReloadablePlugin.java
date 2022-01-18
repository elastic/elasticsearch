/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.settings.Settings;

/**
 * An extension point for {@link Plugin}s that can be reloaded. There is no
 * clear definition about what reloading a plugin actually means. When a plugin
 * is reloaded it might rebuild any internal members. Plugins usually implement
 * this interface in order to reread the values of {@code SecureSetting}s and
 * then rebuild any dependent internal members.
 */
public interface ReloadablePlugin {
    /**
     * Called to trigger the rebuilt of the plugin's internal members. The reload
     * operation <b>is required to have been completed</b> when the method returns.
     * Strictly speaking, the <code>settings</code> argument should not be accessed
     * outside of this method's call stack, as any values stored in the node's
     * keystore (see {@code SecureSetting}) will not otherwise be retrievable. The
     * setting values do not follow dynamic updates, i.e. the values are identical
     * to the ones during the initial plugin loading, barring the keystore file on
     * disk changes. Any failure during the operation should be signaled by raising
     * an exception, but the plugin should otherwise continue to function
     * unperturbed.
     *
     * @param settings
     *            Settings used while reloading the plugin. All values are
     *            retrievable, including the values stored in the node's keystore.
     *            The setting values are the initial ones, from when the node has be
     *            started, i.e. they don't follow dynamic updates.
     * @throws Exception
     *             if the operation failed. The plugin should continue to operate as
     *             if the offending call didn't happen.
     */
    void reload(Settings settings) throws Exception;
}
