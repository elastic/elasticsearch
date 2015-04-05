/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.client;

import org.elasticsearch.watcher.support.http.TemplatedHttpRequest;
import org.elasticsearch.watcher.support.template.ScriptTemplate;

/**
 *
 */
public final class WatchSourceBuilders {

    private WatchSourceBuilders() {
    }

    public static WatchSourceBuilder watchBuilder() {
        return new WatchSourceBuilder();
    }

    public static ScriptTemplate.SourceBuilder template(String text) {
        return new ScriptTemplate.SourceBuilder(text);
    }

    public static TemplatedHttpRequest.SourceBuilder templatedHttpRequest(String host, int port) {
        return new TemplatedHttpRequest.SourceBuilder(host, port);
    }
}
