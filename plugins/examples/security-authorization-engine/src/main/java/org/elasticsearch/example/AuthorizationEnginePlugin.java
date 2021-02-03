/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example;

import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

/**
 * Plugin class that is required so that the code contained here may be loaded as a plugin.
 * Additional items such as settings and actions can be registered using this plugin class.
 */
public class AuthorizationEnginePlugin extends Plugin implements ActionPlugin {
}
