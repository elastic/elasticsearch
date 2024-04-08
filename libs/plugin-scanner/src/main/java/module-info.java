/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.plugin.scanner {
    requires org.elasticsearch.base;
    requires org.objectweb.asm;
    requires org.elasticsearch.plugin;
    requires org.elasticsearch.xcontent;

    exports org.elasticsearch.plugin.scanner;
}
