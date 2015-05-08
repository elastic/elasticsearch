/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class ActionWrapper implements ToXContent {

    private String id;
    private final @Nullable ExecutableTransform transform;
    private final ExecutableAction action;

    public ActionWrapper(String id, ExecutableAction action) {
        this(id, null, action);
    }

    public ActionWrapper(String id, @Nullable ExecutableTransform transform, ExecutableAction action) {
        this.id = id;
        this.transform = transform;
        this.action = action;
    }

    public String id() {
        return id;
    }

    public ExecutableTransform transform() {
        return transform;
    }

    public ExecutableAction action() {
        return action;
    }

    public ActionWrapper.Result execute(WatchExecutionContext ctx) throws IOException {
        Payload payload = ctx.payload();
        Transform.Result transformResult = null;
        if (transform != null) {
            transformResult = transform.execute(ctx, payload);
            payload = transformResult.payload();

        }
        Action.Result actionResult = action.execute(id, ctx, payload);
        return new ActionWrapper.Result(id, transformResult, actionResult);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionWrapper that = (ActionWrapper) o;

        if (!id.equals(that.id)) return false;
        if (transform != null ? !transform.equals(that.transform) : that.transform != null) return false;
        return action.equals(that.action);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (transform != null ? transform.hashCode() : 0);
        result = 31 * result + action.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (transform != null) {
            builder.startObject(Transform.Field.TRANSFORM.getPreferredName())
                    .field(transform.type(), transform, params)
                    .endObject();
        }
        builder.field(action.type(), action, params);
        return builder.endObject();
    }

    static ActionWrapper parse(String watchId, String actionId, XContentParser parser, ActionRegistry actionRegistry, TransformRegistry transformRegistry) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;

        ExecutableTransform transform = null;
        ExecutableAction action = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (Transform.Field.TRANSFORM.match(currentFieldName)) {
                    transform = transformRegistry.parse(watchId, parser);
                } else {
                    // it's the type of the action
                    ActionFactory actionFactory = actionRegistry.factory(currentFieldName);
                    if (actionFactory == null) {
                        throw new ActionException("could not parse action [{}/{}]. unknown action type [{}]", watchId, actionId, currentFieldName);
                    }
                    action = actionFactory.parseExecutable(watchId, actionId, parser);
                }
            }
        }
        if (action == null) {
            throw new ActionException("could not parse watch action [{}/{}]. missing action type", watchId, actionId);
        }
        return new ActionWrapper(actionId, transform, action);
    }

    public static class Result implements ToXContent {

        private final String id;
        private final @Nullable Transform.Result transform;
        private final Action.Result action;

        public Result(String id, Action.Result action) {
            this(id, null, action);
        }

        public Result(String id, @Nullable Transform.Result transform, Action.Result action) {
            this.id = id;
            this.transform = transform;
            this.action = action;
        }

        public String id() {
            return id;
        }

        public Transform.Result transform() {
            return transform;
        }

        public Action.Result action() {
            return action;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Result result = (Result) o;

            if (!id.equals(result.id)) return false;
            if (transform != null ? !transform.equals(result.transform) : result.transform != null) return false;
            return action.equals(result.action);
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + (transform != null ? transform.hashCode() : 0);
            result = 31 * result + action.hashCode();
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.ID.getPreferredName(), id);
            if (transform != null) {
                builder.startObject(Transform.Field.TRANSFORM_RESULT.getPreferredName())
                        .field(transform.type(), transform, params)
                        .endObject();
            }
            builder.field(action.type(), action, params);
            return builder.endObject();
        }

        static Result parse(Wid wid, XContentParser parser, ActionRegistry actionRegistry, TransformRegistry transformRegistry) throws IOException {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT;

            String id = null;
            Transform.Result transformResult = null;
            ActionFactory actionFactory = null;
            BytesReference actionResultSource = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Transform.Field.TRANSFORM.match(currentFieldName)) {
                    transformResult = transformRegistry.parseResult(wid.watchId(), parser);
                } else if (Field.ID.match(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        id = parser.text();
                    } else {
                        throw new ActionException("could not parse action result for watch [{}]. expected a string value for [{}] but found [{}] instead", wid, currentFieldName, token);
                    }
                } else {

                    // it's the type of the action

                    // here we don't directly parse the action type. instead we'll collect
                    // the bytes of the structure that makes the action result. The reason
                    // for this is that we want to make sure to pass the action id to the
                    // action factory when we parse the result (so that error messages will
                    // point to the action result that failed to parse). It's an overhead,
                    // but for worth it for usability purposes.

                    actionFactory = actionRegistry.factory(currentFieldName);
                    if (actionFactory == null) {
                        throw new ActionException("could not parse action result for watch [{}]. unknown action type [{}]", wid, currentFieldName);
                    }

                    // it would have been nice if we had access to the underlying byte offset
                    // of the parser... but for now we'll just need to create a new json
                    // builder with its own (new) byte array and copy over the content.
                    XContentBuilder resultBuilder = jsonBuilder();
                    XContentHelper.copyCurrentStructure(resultBuilder.generator(), parser);
                    actionResultSource = resultBuilder.bytes();
                }
            }

            if (id == null) {
                throw new ActionException("could not parse watch action result for watch [{}]. missing required [{}] field", wid, Field.ID.getPreferredName());
            }

            if (actionFactory == null) {
                throw new ActionException("could not parse watch action result for watch [{}]. missing action result type", wid);
            }

            assert actionResultSource != null : "if we parsed the type name we must have collected the type bytes";

            parser = JsonXContent.jsonXContent.createParser(actionResultSource);
            parser.nextToken();
            Action.Result actionResult = actionFactory.parseResult(wid, id, parser);
            return new Result(id, transformResult, actionResult);
        }
    }

    interface Field {
        ParseField ID = new ParseField("id");
    }
}
