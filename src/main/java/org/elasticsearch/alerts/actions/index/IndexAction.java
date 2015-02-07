/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.AlertContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.ActionException;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.alerts.support.AlertUtils.responseToData;

/**
 */
public class IndexAction extends Action<IndexAction.Result> {

    public static final String TYPE = "index";

    private final ClientProxy client;

    private final String index;
    private final String type;

    protected IndexAction(ESLogger logger, ClientProxy client, String index, String type) {
        super(logger);
        this.client = client;
        this.index = index;
        this.type = type;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(AlertContext ctx, Payload payload) throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.type(type);
        try {
            XContentBuilder resultBuilder = XContentFactory.jsonBuilder().prettyPrint();
            resultBuilder.startObject();
            resultBuilder.field("data", payload.data());
            resultBuilder.field("timestamp", ctx.alert().status().lastExecuted());
            resultBuilder.endObject();
            indexRequest.source(resultBuilder);
        } catch (IOException ioe) {
            logger.error("failed to index result for alert [{}]", ctx.alert().name());
            return new Result(null, "failed ot build index request. " + ioe.getMessage());
        }

        try {
            return new Result(client.index(indexRequest).actionGet(), null);
        } catch (ElasticsearchException e) {
            logger.error("failed to index result for alert [{}]", ctx.alert().name());
            return new Result(null, "failed ot build index request. " + e.getMessage());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.INDEX_FIELD.getPreferredName(), index);
        builder.field(Parser.TYPE_FIELD.getPreferredName(), type);
        builder.endObject();
        return builder;
    }

    public static class Parser extends AbstractComponent implements Action.Parser<IndexAction> {

        public static final ParseField INDEX_FIELD = new ParseField("index");
        public static final ParseField TYPE_FIELD = new ParseField("type");

        private final ClientProxy client;

        @Inject
        public Parser(Settings settings, ClientProxy client) {
            super(settings);
            this.client = client;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public IndexAction parse(XContentParser parser) throws IOException {
            String index = null;
            String type = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (INDEX_FIELD.match(currentFieldName)) {
                        index = parser.text();
                    } else if (TYPE_FIELD.match(currentFieldName)) {
                        type = parser.text();
                    } else {
                        throw new ActionException("could not parse index action. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ActionException("could not parse index action. unexpected token [" + token + "]");
                }
            }

            if (index == null) {
                throw new ActionException("could not parse index action [index] is required");
            }

            if (type == null) {
                throw new ActionException("could not parse index action [type] is required");
            }

            return new IndexAction(logger, client, index, type);
        }
    }

    public static class Result extends Action.Result {

        private final IndexResponse response;
        private final String reason;

        public Result(IndexResponse response, String reason) {
            super(TYPE, response.isCreated());
            this.response = response;
            this.reason = reason;
        }

        public IndexResponse response() {
            return response;
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            if (reason != null) {
                builder.field("reason", reason);
            }
            return builder.field("index_response", responseToData(response()));
        }

    }

}
