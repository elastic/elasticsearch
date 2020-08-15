/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQAccount;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQMessage;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQService;
import org.elasticsearch.xpack.watcher.support.Variables;

import com.rabbitmq.client.ConnectionFactory;

public class ExecutableRabbitMQAction extends ExecutableAction<RabbitMQAction> {
    
    private RabbitMQService rabbitMQService;

    private TextTemplateEngine templateEngine;
    
    public ExecutableRabbitMQAction(RabbitMQAction action, 
            RabbitMQService rabbitMQService, 
            Logger logger,
            TextTemplateEngine templateEngine) {
        super(action, logger);
        this.templateEngine = templateEngine;
        this.rabbitMQService = rabbitMQService;
    }
    
    @Override
    public Action.Result execute(final String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        RabbitMQAccount account = rabbitMQService.getAccount(action.account);
        if (account == null) {
            // the account associated with this action was deleted
            throw new IllegalStateException("account [" + action.account + "] was not found. perhaps it was deleted");
        }

        Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);
        
        String vhost = ConnectionFactory.DEFAULT_VHOST;
        if(action.vhost != null) {
            vhost = templateEngine.render(new TextTemplate(action.vhost), model);
        }
        
        String exchange = "";
        if(action.exchange != null) {
            exchange = templateEngine.render(new TextTemplate(action.exchange), model);
        }
        
        String routingKey = null;
        if(action.routingKey != null) {
            routingKey = templateEngine.render(new TextTemplate(action.routingKey), model);
        }
        
        Map<String, Object> headers = new HashMap<>();
        if(action.headers != null) {
            for(Map.Entry<String, String> header : action.headers.entrySet()) {
                headers.put(header.getKey(),  
                        templateEngine.render(new TextTemplate(header.getValue()), model));
            }
        }
        
        String message = templateEngine.render(new TextTemplate(action.message), model);
        
        account.sendMessage(vhost, exchange, routingKey, headers, message.getBytes(StandardCharsets.UTF_8));
        
        RabbitMQMessage response = new RabbitMQMessage(action.account, vhost, exchange, routingKey, headers, message);

        if (ctx.simulateAction(actionId)) {
            return new RabbitMQAction.Result.Simulated(response);
        }
        
        return new RabbitMQAction.Result.Success(response);
    }
    
}
