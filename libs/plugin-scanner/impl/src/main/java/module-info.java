/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.plugin.scanner.impl {
    requires org.objectweb.asm;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    requires org.elasticsearch.logging;
    requires org.apache.logging.log4j;
    requires org.elasticsearch.plugin.api;
    requires org.elasticsearch.base;

    requires org.elasticsearch.plugin.scanner;

    exports org.elasticsearch.plugin.scanner.impl to org.elasticsearch.plugin.scanner;

    provides org.elasticsearch.plugin.scanner.spi.StablePluginRegistryProvider
        with
            org.elasticsearch.plugin.scanner.impl.StablePluginRegistryProviderImpl;
}
