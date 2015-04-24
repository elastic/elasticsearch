/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

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
            if (transform != null) {
                builder.startObject(Transform.Field.TRANSFORM_RESULT.getPreferredName())
                        .field(transform.type(), transform, params)
                        .endObject();
            }
            builder.field(action.type(), action, params);
            return builder.endObject();
        }

        static Result parse(Wid wid, String actionId, XContentParser parser, ActionRegistry actionRegistry, TransformRegistry transformRegistry) throws IOException {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT;

            Transform.Result transformResult = null;
            Action.Result actionResult = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if (Transform.Field.TRANSFORM.match(currentFieldName)) {
                        transformResult = transformRegistry.parseResult(wid.watchId(), parser);
                    } else {
                        // it's the type of the action
                        ActionFactory actionFactory = actionRegistry.factory(currentFieldName);
                        if (actionFactory == null) {
                            throw new ActionException("could not parse action result [{}/{}]. unknown action type [{}]", wid, actionId, currentFieldName);
                        }
                        actionResult = actionFactory.parseResult(wid, actionId, parser);
                    }
                }
            }
            if (actionResult == null) {
                throw new ActionException("could not parse watch action result [{}/{}]. missing action result type", wid, actionId);
            }
            return new Result(actionId, transformResult, actionResult);
        }
    }

}
