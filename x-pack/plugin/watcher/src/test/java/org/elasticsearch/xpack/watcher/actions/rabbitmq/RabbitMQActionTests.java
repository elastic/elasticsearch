/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.rabbitmq;

import static org.elasticsearch.common.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

public class RabbitMQActionTests extends ESTestCase {
    
    private final String TEST_EXCHANGE = "test_exchange";
    
    private final String TEST_ROUTING_KEY = "test_routing_key";
    
    private final String TEST_MESSAGE = "test message";
    
    public void testParser() throws Exception {
        final String accountName = randomAlphaOfLength(10);
        HashMap<String, String> headers = new HashMap<>();
        headers.put("test_header", "test_header_value");
        
        XContentBuilder builder = jsonBuilder().startObject()
                    .field("account", accountName)
                    .field("exchange", TEST_EXCHANGE)
                    .field("routingKey", TEST_ROUTING_KEY)
                    .field("headers", headers)
                    .field("message", TEST_MESSAGE)
                .endObject();
        
        BytesReference bytes = BytesReference.bytes(builder);
        logger.info("rabbitmq action json [{}]", bytes.utf8ToString());

        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        RabbitMQAction action = RabbitMQAction.parse("_watch", "_action", parser);

        assertThat(action, notNullValue());
        assertThat(action.account, is(accountName));
        assertThat(action.exchange, is(TEST_EXCHANGE));
        assertThat(action.routingKey, is(TEST_ROUTING_KEY));
        assertThat(action.headers.size(), is(1));
        assertThat(action.headers.get("test_header"), is("test_header_value"));
    }
    
    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("unknown_field", "value").endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> RabbitMQAction.parse("_w", "_a", parser));
        assertThat(e.getMessage(), is("failed to parse [rabbitmq] action [_w/_a]. unexpected token [VALUE_STRING/unknown_field]"));
    }

    public void testToXContent() throws Exception {
        final RabbitMQAction action = randomRabbitMQAction();

        try (XContentBuilder builder = randomFrom(jsonBuilder(), smileBuilder(), yamlBuilder(), cborBuilder())) {
            action.toXContent(builder, ToXContent.EMPTY_PARAMS);

            String parsedAccount = null;

            String parsedExchange = null;
            String parsedRoutingKey = null;
            Map<String, String> parsedHeaders = null;
            String parsedMessage = null;
            String currentFieldName = null;

            try (XContentParser parser = createParser(builder)) {
                assertNull(parser.currentToken());
                parser.nextToken();

                XContentParser.Token token = parser.currentToken();
                assertThat(token, is(XContentParser.Token.START_OBJECT));

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if ("account".equals(currentFieldName)) {
                        parsedAccount = parser.text();
                    } else if ("exchange".equals(currentFieldName)) {
                        parsedExchange = parser.text();
                    } else if ("routingKey".equals(currentFieldName)) {
                        parsedRoutingKey = parser.text();
                    } else if ("headers".equals(currentFieldName)) {
                        parsedHeaders = parser.mapStrings();
                    } else if ("message".equals(currentFieldName)) {
                        parsedMessage = parser.text();
                    } else {
                        fail("unknown field [" + currentFieldName + "]");
                    }
                }
            }

            assertThat(parsedAccount, equalTo(action.account));
            assertThat(parsedExchange, equalTo(action.exchange));
            assertThat(parsedRoutingKey, equalTo(action.routingKey));
            assertThat(parsedHeaders.size(), equalTo(action.headers.size()));
            if(parsedHeaders.size() > 0) {
                Map.Entry<String, String> entry = parsedHeaders.entrySet().iterator().next();
                Map.Entry<String, String> actionEntry = action.headers.entrySet().iterator().next();
                
                assertThat(entry.getKey(), equalTo(actionEntry.getKey()));
                assertThat(entry.getValue(), equalTo(actionEntry.getValue()));
            }
            assertThat(parsedMessage, equalTo(action.message));
        }
    }
    
    private static RabbitMQAction randomRabbitMQAction() {
        
        String account = null;
        if (randomBoolean()) {
            account = randomAlphaOfLength(randomIntBetween(5, 10));
        }
        
        String vhost = null;
        if (randomBoolean()) {
            vhost = randomFrom("/", "test_vhost1", "test_vhost2");
        }
        
        String exchange = null;
        if (randomBoolean()) {
            exchange = randomFrom("first_exchange", "second_exchange", "third_exchange");
        }
        
        String routingKey = null;
        if (randomBoolean()) {
            routingKey = randomFrom("first_key", "second_key", "third_key");
        }
        
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            String key = randomFrom("key1", "key2", "key3");
            String value = randomFrom("value1", "value2", "value3");
            headers.put(key, value);
        }

        String message = null;
        if (randomBoolean()) {
            message = randomFrom("first_message", "second_message", "third_message");
        }

        return new RabbitMQAction(account, vhost, exchange, routingKey, headers, message);
    }
    
    /**
     * TextTemplateEngine that picks up templates from the model if exist,
     * otherwise returns the template as it is.
     */
    class ModelTextTemplateEngine extends TextTemplateEngine {

        private final Map<String, Object> model;

        ModelTextTemplateEngine(Map<String, Object> model) {
            super(mock(ScriptService.class));
            this.model = model;
        }

        @Override
        public String render(TextTemplate textTemplate, Map<String, Object> ignoredModel) {
            String template = textTemplate.getTemplate();
            if (model.containsKey(template)) {
                return (String) model.get(template);
            }
            return template;
        }
    }
}
