/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.rabbitmq;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.notification.rabbitmq.RabbitMQMessage;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQAction implements Action {

    public static final String TYPE = "rabbitmq";
    
    final String account;
    @Nullable final String vhost;
    @Nullable final String exchange;
    @Nullable final String routingKey;
    @Nullable final Map<String, String> headers;
    final String message;

    public RabbitMQAction(String account,
            @Nullable String vhost, 
            @Nullable String exchange, 
            @Nullable String routingKey, 
            @Nullable Map<String, String> headers,
            String message) {
        this.account = account;
        this.exchange = exchange != null ? exchange : "";
        this.vhost = vhost != null ? vhost : ConnectionFactory.DEFAULT_VHOST;
        this.routingKey = routingKey;
        this.headers = headers;
        this.message = message;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RabbitMQAction action = (RabbitMQAction) o;

        return Objects.equals(account, action.account) &&
                Objects.equals(vhost, action.vhost) &&
                Objects.equals(exchange, action.exchange) &&
                Objects.equals(routingKey, action.routingKey) &&
                Objects.equals(headers, action.headers) && 
                Objects.deepEquals(message, action.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, vhost, exchange, routingKey, headers, message);
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        
        if (account != null) {
            builder.field(Field.ACCOUNT.getPreferredName(), account);
        }
        if (vhost != null) {
            builder.field(Field.VHOST.getPreferredName(), exchange);
        }
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

    public static RabbitMQAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String account = null;
        String vhost = null;
        String exchange = null;
        String routingKey = null;
        Map<String, String> headers = null;
        String message = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.ACCOUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    account = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but " +
                            "found [{}] instead", TYPE, watchId, actionId, Field.ACCOUNT.getPreferredName(), token);
                }
            } else if (Field.VHOST.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    vhost = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but " +
                            "found [{}] instead", TYPE, watchId, actionId, Field.VHOST.getPreferredName(), token);
                }
            } else if (Field.EXCHANGE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    exchange = parser.text();
                } else {
                    throw new ElasticsearchParseException(
                            "failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but "
                                    + "found [{}] instead",
                            TYPE, watchId, actionId, Field.EXCHANGE.getPreferredName(), token);
                }
            }
            else if (Field.ROUTING_KEY.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    routingKey = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but " +
                            "found [{}] instead", TYPE, watchId, actionId, Field.ROUTING_KEY.getPreferredName(), token);
                }
            } else if (Field.MESSAGE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    message = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but " +
                            "found [{}] instead", TYPE, watchId, actionId, Field.MESSAGE.getPreferredName(), token);
                }
            } else if (Field.HEADERS.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    headers = parser.mapStrings();
                } catch (Exception e) {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. failed to parse [{}] field", e, TYPE,
                            watchId, actionId, Field.HEADERS.getPreferredName());
                }
            } else {
                throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. unexpected token [{}/{}]", TYPE, watchId,
                        actionId, token, currentFieldName);
            }
        }
        return new RabbitMQAction(account, vhost, exchange, routingKey, headers, message);
    }

    public static class Builder implements Action.Builder<RabbitMQAction> {

        final RabbitMQAction action;

        public Builder(RabbitMQAction action) {
            this.action = action;
        }

        @Override
        public RabbitMQAction build() {
            return action;
        }
    }

    public static Builder builder(String account,
            String vhost,
            String exchange, 
            String routingKey, 
            Map<String, String> headers,
            String message) {
        return new Builder(new RabbitMQAction(account, vhost, exchange, routingKey, headers, message));
    }
    
    public interface Result {

        class Success extends Action.Result implements Result {

            private RabbitMQMessage message;
            
            public Success(RabbitMQMessage message) {
                super(TYPE, Status.SUCCESS);
                this.message = message;
            }
            
            public RabbitMQMessage getMessage() {
                return message;
            }
            
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                XContentBuilder result = builder.startObject(type)
                        .field(Field.MESSAGE.getPreferredName(), message, params);
                
                return result.endObject();
            }
        }

        class Simulated extends Action.Result implements Result {

             private RabbitMQMessage message;
             
             public Simulated(RabbitMQMessage message) {
                 super(TYPE, Status.SIMULATED);
                 this.message = message;
             }
             
             public RabbitMQMessage getMessage() {
                return message;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                XContentBuilder result = builder.startObject(type)
                         .field(Field.MESSAGE.getPreferredName(), message, params);
                 return result.endObject();
            }
        }
    }
    
    public interface Field {
        ParseField ACCOUNT = new ParseField("account");
        ParseField VHOST = new ParseField("vhost");
        ParseField EXCHANGE = new ParseField("exchange");
        ParseField ROUTING_KEY = new ParseField("routingKey");
        ParseField HEADERS = new ParseField("headers");
        ParseField MESSAGE = new ParseField("message");
    }
    
}
