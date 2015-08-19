/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.watcher.support.template.xmustache.XMustacheTemplateEngine;

/**
 *
 */
public class TemplateModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(XMustacheTemplateEngine.class).asEagerSingleton();
        bind(TemplateEngine.class).to(XMustacheTemplateEngine.class);
    }
}
