/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.actions.ActionSettingsException;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class IndexAction extends Action<IndexAction.Result> {

    public static final String TYPE = "index";

    private final ClientProxy client;

    private final String index;
    private final String type;

    public IndexAction(ESLogger logger, @Nullable Transform transform, ClientProxy client, String index, String type) {
        super(logger, transform);
        this.client = client;
        this.index = index;
        this.type = type;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    protected Result execute(WatchExecutionContext ctx, Payload payload) throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.type(type);
        try {
            XContentBuilder resultBuilder = XContentFactory.jsonBuilder().prettyPrint();
            resultBuilder.startObject();
            resultBuilder.field("data", payload.data());
            resultBuilder.field("timestamp", ctx.executionTime());
            resultBuilder.endObject();
            indexRequest.source(resultBuilder);
        } catch (IOException ioe) {
            logger.error("failed to index result for watch [{}]", ioe, ctx.watch().name());
            return new Result(null, "failed to build index request. " + ioe.getMessage(), false);
        }

        try {
            IndexResponse response = client.index(indexRequest).actionGet();
            Map<String,Object> data = new HashMap<>();
            data.put("created", response.isCreated());
            data.put("id", response.getId());
            data.put("version", response.getVersion());
            data.put("type", response.getType());
            data.put("index", response.getIndex());
            return new Result(new Payload.Simple(data), null, response.isCreated());
        } catch (ElasticsearchException e) {
            logger.error("failed to index result for watch [{}]", e, ctx.watch().name());
            return new Result(null, "failed to build index request. " + e.getMessage(), false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (transform != null) {
            builder.startObject(Transform.Parser.TRANSFORM_FIELD.getPreferredName())
                    .field(transform.type(), transform)
                    .endObject();
        }
        builder.field(Parser.INDEX_FIELD.getPreferredName(), index);
        builder.field(Parser.TYPE_FIELD.getPreferredName(), type);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexAction that = (IndexAction) o;

        if (index != null ? !index.equals(that.index) : that.index != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (transform != null ? !transform.equals(that.transform) : that.transform != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (transform != null ? transform.hashCode() : 0);
        return result;
    }

    public static class Parser extends AbstractComponent implements Action.Parser<Result, IndexAction> {

        public static final ParseField INDEX_FIELD = new ParseField("index");
        public static final ParseField TYPE_FIELD = new ParseField("type");
        public static final ParseField REASON_FIELD = new ParseField("reason");
        public static final ParseField RESPONSE_FIELD = new ParseField("response");

        private final ClientProxy client;
        private final TransformRegistry transformRegistry;

        @Inject
        public Parser(Settings settings, ClientProxy client, TransformRegistry transformRegistry) {
            super(settings);
            this.client = client;
            this.transformRegistry = transformRegistry;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public IndexAction parse(XContentParser parser) throws IOException {
            String index = null;
            String type = null;
            Transform transform = null;

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
                        throw new ActionSettingsException("could not parse index action. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Transform.Parser.TRANSFORM_FIELD.match(currentFieldName)) {
                        transform = transformRegistry.parse(parser);
                    } else {
                        throw new ActionSettingsException("could not parse index action. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ActionSettingsException("could not parse index action. unexpected token [" + token + "]");
                }
            }

            if (index == null) {
                throw new ActionSettingsException("could not parse index action [index] is required");
            }

            if (type == null) {
                throw new ActionSettingsException("could not parse index action [type] is required");
            }

            return new IndexAction(logger, transform, client, index, type);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Transform.Result transformResult = null;
            Boolean success = null;
            Payload payload = null;
            String reason = null;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (REASON_FIELD.match(currentFieldName)) {
                        reason = parser.text();
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Action.Result.SUCCESS_FIELD.match(currentFieldName)) {
                            success = parser.booleanValue();
                        } else {
                            throw new ActionException("could not parse index result. unexpected boolean field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ActionException("could not parse index result. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (RESPONSE_FIELD.match(currentFieldName)) {
                        payload = new Payload.Simple(parser.map());
                    } else if (Transform.Parser.TRANSFORM_RESULT_FIELD.match(currentFieldName)) {
                        transformResult = transformRegistry.parseResult(parser);
                    } else {
                        throw new ActionException("could not parse index result. unexpected object field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ActionException("could not parse index result. unexpected token [" + token + "]");
                }
            }

            if (success == null) {
                throw new ActionException("could not parse index result. expected boolean field [success]");
            }

            Result result = new Result(payload, reason, success);
            if (transformResult != null) {
                result.transformResult(transformResult);
            }
            return result;
        }
    }

    public static class Result extends Action.Result {

        private final Payload response;
        private final String reason;

        public Result(Payload response, String reason, boolean isCreated) {
            super(TYPE, isCreated);
            this.response = response;
            this.reason = reason;
        }

        void transformResult(Transform.Result result) {
            this.transformResult = result;
        }

        public Payload response() {
            return response;
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            if (reason != null) {
                builder.field(Parser.REASON_FIELD.getPreferredName(), reason);
            }
            if (response != null) {
                builder.field(Parser.RESPONSE_FIELD.getPreferredName(), response());
            }
            return builder;
        }
    }

    public static class SourceBuilder implements Action.SourceBuilder {

        private final String index;
        private final String type;

        public SourceBuilder(String index, String type) {
            this.index = index;
            this.type = type;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field(Parser.INDEX_FIELD.getPreferredName(), index)
                    .field(Parser.TYPE_FIELD.getPreferredName(), type)
                    .endObject();
        }
    }

}


