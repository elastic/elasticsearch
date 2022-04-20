/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;

import java.util.Map;

public interface BuiltinTemplatePlugin {

    default Map<String, ComponentTemplate> getComponentTemplates() {
        return Map.of();
    }

    default Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return Map.of();
    }

    default String getOrigin() {
        return "";
    }

}
