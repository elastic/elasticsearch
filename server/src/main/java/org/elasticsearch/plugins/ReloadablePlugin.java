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