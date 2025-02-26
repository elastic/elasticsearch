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

public class TemplateScriptBridge {
    public static class Factory extends StableBridgeAPI.Proxy<TemplateScript.Factory> {
        public static Factory wrap(final TemplateScript.Factory delegate) {
            return new Factory(delegate);
        }

        public Factory(final TemplateScript.Factory delegate) {
            super(delegate);
        }

        @Override
        public TemplateScript.Factory unwrap() {
            return this.delegate;
        }
    }
}
