/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ReferenceDocs;

/**
 * Bootstrap check that prevents a stateless node from starting without a tika-server configured.
 *
 * <p>Running Apache Tika in-process on a shared stateless node is unsafe: a malicious document
 * (e.g. a zip bomb or OOM-inducing PDF) can crash the entire JVM. In stateless mode nodes share
 * infrastructure, so an attachment processor must delegate to an external tika-server instead.
 */
final class TikaServerBootstrapCheck implements BootstrapCheck {

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        if (DiscoveryNode.isStateless(context.settings())
            && IngestAttachmentPlugin.TIKA_SERVER_URL_SETTING.get(context.settings()).isEmpty()
            && IngestAttachmentPlugin.ALLOW_LOCAL_IN_STATELESS_SETTING.get(context.settings()) == false) {
            return BootstrapCheckResult.failure(
                "The attachment processor is loaded and this node is running in stateless mode. "
                    + "In stateless mode the attachment processor must use an external tika-server to avoid crashing the JVM "
                    + "with malicious documents. Set ["
                    + IngestAttachmentPlugin.TIKA_SERVER_URL_SETTING.getKey()
                    + "] to the URL of a running tika-server instance."
            );
        }
        return BootstrapCheckResult.success();
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }

    @Override
    public ReferenceDocs referenceDocs() {
        return ReferenceDocs.BOOTSTRAP_CHECK_TIKA_SERVER;
    }
}
