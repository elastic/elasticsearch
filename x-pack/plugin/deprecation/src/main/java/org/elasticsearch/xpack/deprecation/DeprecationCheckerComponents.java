/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

public class DeprecationCheckerComponents {

    private final NamedXContentRegistry xContentRegistry;
    private final Settings settings;
    private final Client client;

    DeprecationCheckerComponents(NamedXContentRegistry xContentRegistry, Settings settings, Client client) {
        this.xContentRegistry = xContentRegistry;
        this.settings = settings;
        this.client = client;
    }

    public NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public Settings settings() {
        return settings;
    }

    public Client client() {
        return client;
    }

}
