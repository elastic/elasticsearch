/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.inject;

/**
 * A module can implement this interface to allow to pre process other modules
 * before an injector is created.
 *
 *
 */
public interface PreProcessModule {

    void processModule(Module module);
}
