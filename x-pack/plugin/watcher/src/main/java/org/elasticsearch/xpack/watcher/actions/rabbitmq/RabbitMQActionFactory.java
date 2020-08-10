/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.rabbitmq;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQService;

public class RabbitMQActionFactory extends ActionFactory {

    private final TextTemplateEngine templateEngine;

    private RabbitMQService rabbitMQService;
    
    public RabbitMQActionFactory(TextTemplateEngine templateEngine, 
            RabbitMQService rabbitMQService) {
        super(LogManager.getLogger(ExecutableRabbitMQAction.class));
        this.templateEngine = templateEngine;
        this.rabbitMQService = rabbitMQService;
    }
    
    @Override
    public ExecutableRabbitMQAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        RabbitMQAction action = RabbitMQAction.parse(watchId, actionId, parser);
        return new ExecutableRabbitMQAction(action, rabbitMQService, actionLogger, templateEngine);
    }
}
