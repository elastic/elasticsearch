/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.launcher;

public interface ToolProvider {

    // TODO: merge launcher and cli lib, move to subdir in distro, make compileOnly dep in server

    // TODO: this is temporary
    void main(String[] args, Terminal terminal) throws Exception;
}
