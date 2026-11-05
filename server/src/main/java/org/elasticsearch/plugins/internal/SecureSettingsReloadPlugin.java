/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.plugins.ReloadablePlugin;

/**
 * A plugin that takes over reloading {@link ReloadablePlugin}s for {@code POST _nodes/reload_secure_settings}, so it can combine the
 * keystore with secure settings from other sources. Without this, a keystore reload would drop every secure setting that doesn't come
 * from the keystore. At most one plugin may implement this.
 */
public interface SecureSettingsReloadPlugin {

    /**
     * Reloads {@code reloadablePlugins} with settings that include the given keystore. Runs on the thread of the reload request and
     * may block.
     *
     * @param keystore          the keystore just read and decrypted; closed by the caller after this method returns
     * @param reloadablePlugins reloads every {@link ReloadablePlugin} on the node and throws if any of them fails
     */
    void reloadWithKeystore(SecureSettings keystore, ReloadablePlugin reloadablePlugins) throws Exception;
}
