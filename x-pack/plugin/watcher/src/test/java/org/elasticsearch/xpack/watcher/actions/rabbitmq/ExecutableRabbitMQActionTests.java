/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.rabbitmq;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.Action.Result.Status;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.jira.JiraAccount;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQAccount;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQMessage;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQService;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ExecutableRabbitMQActionTests extends ESTestCase {

    private final String TEST_VHOST = "test_vhost";
    
    private final String TEST_EXCHANGE = "test_exchange";
    
    private final String TEST_ROUTING_KEY = "test_routing_key";
    
    private final String TEST_MESSAGE = "test message";
    
    public void testExecutableActionStatus() throws Exception {
        
        ConnectionFactory connectionFactory = createMockFactory();
        
        RabbitMQAction action = new RabbitMQAction("account1", TEST_VHOST, TEST_EXCHANGE, TEST_ROUTING_KEY, null, TEST_MESSAGE);

        final String host = randomFrom("localhost", "internal-rabbitmq.elastic.co");
        final int port = randomFrom(5672, 5673);
        final String url = "https://" + host + ":" + port;
        final String user = randomAlphaOfLength(10);
        final String password = randomAlphaOfLength(10);
        
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RabbitMQAccount.SECURE_URL_SETTING.getKey(), url);
        secureSettings.setString(RabbitMQAccount.SECURE_USER_SETTING.getKey(), user);
        secureSettings.setString(RabbitMQAccount.SECURE_PASSWORD_SETTING.getKey(), password);
        Settings accountSettings = Settings.builder().setSecureSettings(secureSettings).build();

        RabbitMQAccount account = new RabbitMQAccount("account1", accountSettings, connectionFactory);

        RabbitMQService service = mock(RabbitMQService.class);
        when(service.getAccount(eq("account1"))).thenReturn(account);

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        Wid wid = new Wid(randomAlphaOfLength(5), now);
        WatchExecutionContext ctx = mockExecutionContextBuilder(wid.watchId())
                .wid(wid)
                .payload(new Payload.Simple())
                .time(wid.watchId(), now)
                .buildMock();

        ExecutableRabbitMQAction executable = new ExecutableRabbitMQAction(action, service, logger, new UpperCaseTextTemplateEngine());
        Action.Result result = executable.execute("foo", ctx, new Payload.Simple());
        
        assertThat(result.type(), is("rabbitmq"));
        assertThat(result.status(), is(Status.SUCCESS));
    }

    public void testExecutionResult() throws Exception {
        
        Map<String, String> headers = new HashMap<>();
        headers.put("test_header", "value");
        
        RabbitMQAction.Result.Simulated result = simulateExecution(TEST_VHOST, TEST_EXCHANGE, 
                TEST_ROUTING_KEY,
                headers, TEST_MESSAGE, emptyMap());
        assertEquals(result.status(), Status.SIMULATED);
        RabbitMQMessage message = result.getMessage();
        assertEquals(message.getExchange(), TEST_EXCHANGE.toUpperCase(Locale.ROOT));
        assertEquals(message.getRoutingKey(), TEST_ROUTING_KEY.toUpperCase(Locale.ROOT));
        assertEquals(message.getMessage(), TEST_MESSAGE.toUpperCase(Locale.ROOT));
        assertEquals(message.getHeaders().size(), 1);
        assertEquals(message.getHeaders().get("test_header"), "VALUE");
    }
    
    private RabbitMQAction.Result.Simulated simulateExecution(String vhost, String exchange, String routingKey, 
            Map<String, String> headers, String message, Map<String, String> accountFields
            ) throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(JiraAccount.SECURE_URL_SETTING.getKey(), "https://localhost:443");
        secureSettings.setString(JiraAccount.SECURE_USER_SETTING.getKey(), "elastic");
        secureSettings.setString(JiraAccount.SECURE_PASSWORD_SETTING.getKey(), "secret");
        Settings.Builder settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .putProperties(accountFields, s -> "message_defaults." + s);

        RabbitMQAccount account = new RabbitMQAccount("account", settings.build(), 
                createMockFactory());
        
        RabbitMQService service = mock(RabbitMQService.class);
        when(service.getAccount(eq("account"))).thenReturn(account);

        RabbitMQAction action = new RabbitMQAction("account", vhost, exchange, routingKey, headers, message);
        ExecutableRabbitMQAction executable = new ExecutableRabbitMQAction(action, service, null, new UpperCaseTextTemplateEngine());

        WatchExecutionContext context = createWatchExecutionContext();
        when(context.simulateAction("test")).thenReturn(true);

        Action.Result result = executable.execute("test", context, new Payload.Simple());
        assertThat(result, instanceOf(RabbitMQAction.Result.class));
        assertThat(result, instanceOf(RabbitMQAction.Result.Simulated.class));
        return (RabbitMQAction.Result.Simulated) result;
    }

    private ConnectionFactory createMockFactory() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        return connectionFactory;
    }

    private WatchExecutionContext createWatchExecutionContext() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Wid wid = new Wid(randomAlphaOfLength(5), now);
        Map<String, Object> metadata = MapBuilder.<String, Object>newMapBuilder().put("_key", "_val").map();
        return mockExecutionContextBuilder("watch1")
                .wid(wid)
                .payload(new Payload.Simple())
                .time("watch1", now)
                .metadata(metadata)
                .buildMock();
    }
    
    /**
     * TextTemplateEngine that convert templates to uppercase
     */
    class UpperCaseTextTemplateEngine extends TextTemplateEngine {

        UpperCaseTextTemplateEngine() {
            super(mock(ScriptService.class));
        }

        @Override
        public String render(TextTemplate textTemplate, Map<String, Object> model) {
            return textTemplate.getTemplate().toUpperCase(Locale.ROOT);
        }
    }
    
}
