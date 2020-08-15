/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.rabbitmq;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.rabbitmqAction;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQAccount;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQService;
import org.junit.Before;

public class RabbitMQActionFactoryTests extends ESTestCase {

    private RabbitMQService service;

    @Before
    public void init() throws Exception {
        service = mock(RabbitMQService.class);
    }
    
    public void testParseAction() throws Exception {
        RabbitMQAccount account = mock(RabbitMQAccount.class);
        when(service.getAccount("_account1")).thenReturn(account);

        RabbitMQAction action = rabbitmqAction("_account1", "test_vhost", "test_exchange", "test_queue", 
                null, "test message").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = createParser(jsonBuilder);
        parser.nextToken();

        RabbitMQAction parsedAction = RabbitMQAction.parse("_w1", "_a1", parser);
        assertThat(parsedAction, equalTo(action));
    }
}
