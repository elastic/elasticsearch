/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

public abstract class NodeConfigurationSource {

    public static final NodeConfigurationSource EMPTY = new NodeConfigurationSource() {
        @Override
        public Settings nodeSettings(int nodeOrdinal, Settings settings) {
            return Settings.EMPTY;
        }

        @Override
        public Path nodeConfigPath(int nodeOrdinal) {
            return null;
        }
    };

    /**
     * The settings for the node represented by the given ordinal, or null if there are no settings defined. The provided
     * {@code otherSettings} are the override settings that the test framework will unconditionally apply to the settings returned by this
     * method. These settings are provided so that implementors can make informed decisions about the values of settings they might wish to
     * return, that would be invalid with certain combinations of the override settings that the test framework will apply.
     * <p>
     * For example, imagine that some setting values are invalid if the node is a master-only node. The implementor can inspect the
     * {@code otherSettings} to determine that the test framework is going to override the role settings for the node being constructed to
     * be a master-only node, and therefore the implementor can ensure that it only returns valid values for the master-only node being
     * constructed.
     *
     * @param nodeOrdinal   the ordinal of the node being constructed
     * @param otherSettings the override settings
     * @return the settings for this node
     */
    public abstract Settings nodeSettings(int nodeOrdinal, Settings otherSettings);

    public abstract Path nodeConfigPath(int nodeOrdinal);

    /** Returns plugins that should be loaded on the node */
    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }
}
