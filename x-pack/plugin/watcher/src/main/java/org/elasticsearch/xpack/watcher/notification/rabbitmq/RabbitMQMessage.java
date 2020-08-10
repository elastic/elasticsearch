/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.rabbitmq;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class RabbitMQMessage implements ToXContentObject {
    
    final String account;
    @Nullable final String exchange;
    @Nullable final String routingKey;
    @Nullable final Map<String, Object> headers;
    String message;
    
    public RabbitMQMessage(String account, 
            String exchange, 
            String routingKey, 
            Map<String, Object> headers,
            String message) {
        this.account = account;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.headers = headers;
        this.message = message;
    }
    
    public String getAccount() {
        return account;
    }

    public String getExchange() {
        return exchange;
    }
    
    public Map<String, Object> getHeaders() {
        return headers;
    }
    
    public String getRoutingKey() {
        return routingKey;
    }
    
    public String getMessage() {
        return message;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RabbitMQMessage issue = (RabbitMQMessage) o;
        return Objects.equals(account, issue.account) &&
                Objects.equals(exchange, issue.exchange) &&
                Objects.equals(routingKey, issue.routingKey) &&
                Objects.equals(headers, issue.headers) && 
                Objects.equals(message, issue.message);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(account, exchange, routingKey, headers, message);
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.ACCOUNT.getPreferredName(), account);
        if (exchange != null) {
            builder.field(Field.EXCHANGE.getPreferredName(), exchange);
        }
        if (routingKey != null) {
            builder.field(Field.ROUTING_KEY.getPreferredName(), routingKey);
        }
        if (headers != null) {
            builder.field(Field.HEADERS.getPreferredName(), headers);
        }
        if (message != null) {
            builder.field(Field.MESSAGE.getPreferredName(), message);
        }

        return builder.endObject();
    }
    
    private interface Field {
        ParseField ACCOUNT = new ParseField("account");
        ParseField EXCHANGE = new ParseField("exchange");
        ParseField ROUTING_KEY = new ParseField("routingKey");
        ParseField HEADERS = new ParseField("headers");
        ParseField MESSAGE = new ParseField("headers");
    }
}
