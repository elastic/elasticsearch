/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ReloadablePlugin;

/**
 * This interface allows adding support for reload operations (on secure settings change) in a generic way for security components.
 * The implementors of this interface will be called when the values of {@code SecureSetting}s should be reloaded by security plugin.
 * For more information about reloading plugin secure settings, see {@link ReloadablePlugin}.
 */
public interface ReloadableSecurityComponent {

    /**
     * Called when a reload security settings action is executed. The reload operation
     * <b>must be completed</b> when this method returns. Strictly speaking, the
     * <code>settings</code> argument should not be accessed outside of this method's
     * call stack, as any values stored in the node's keystore (see {@code SecureSetting})
     * will not otherwise be retrievable.
     * <p>
     * There is no guarantee that the secure setting's values have actually changed.
     * Hence, it's up to implementor to detect if the actual internal reloading is
     * necessary.
     * <p>
     * Any failure during the reloading should be signaled by raising an exception.
     * <p>
     * For additional info, see also: {@link ReloadablePlugin#reload(Settings)}.
     *
     * @param settings
     *            Settings include the initial node's settings and all decrypted
     *            secure settings from the keystore. Absence of a particular secure
     *            setting may mean that the setting was either never configured or
     *            that it was simply removed.
     */
    void reload(Settings settings);

}
