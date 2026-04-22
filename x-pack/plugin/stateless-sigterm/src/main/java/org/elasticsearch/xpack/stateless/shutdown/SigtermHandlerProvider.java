/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.node.internal.TerminationHandlerProvider;

/**
 * This is the class that's actually injected via SPI. Plumbing.
 */
public class SigtermHandlerProvider implements TerminationHandlerProvider {
    private final StatelessSigtermPlugin plugin;

    public SigtermHandlerProvider() {
        throw new IllegalStateException(this.getClass().getSimpleName() + " must be constructed using PluginsService");
    }

    public SigtermHandlerProvider(StatelessSigtermPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public TerminationHandler handler() {
        return this.plugin.getTerminationHandler();
    }
}
