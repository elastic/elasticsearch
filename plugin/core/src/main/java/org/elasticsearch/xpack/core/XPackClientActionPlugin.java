/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.common.settings.Settings;

public interface XPackClientActionPlugin {

    static boolean isTribeNode(Settings settings) {
        return settings.getGroups("tribe", true).isEmpty() == false;
    }

    static boolean isTribeClientNode(Settings settings) {
        return settings.get("tribe.name") != null;
    }
}
