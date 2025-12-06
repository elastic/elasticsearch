/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.script;

import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.script.TemplateScript;

/**
 * An external bridge for {@link TemplateScript.Factory}
 */
public interface TemplateScriptFactoryBridge extends StableBridgeAPI<TemplateScript.Factory> {
    static TemplateScriptFactoryBridge fromInternal(final TemplateScript.Factory delegate) {
        return new ProxyInternal(delegate);
    }

    /**
     * An implementation of {@link TemplateScriptFactoryBridge} that proxies calls through
     * to an internal {@link TemplateScript.Factory}.
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<TemplateScript.Factory> implements TemplateScriptFactoryBridge {
        ProxyInternal(final TemplateScript.Factory delegate) {
            super(delegate);
        }
    }
}
