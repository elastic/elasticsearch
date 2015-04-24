/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.script;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class ScriptTransform implements Transform {

    public static final String TYPE = "script";

    private final Script script;

    public ScriptTransform(Script script) {
        this.script = script;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public Script getScript() {
        return script;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTransform that = (ScriptTransform) o;

        return script.equals(that.script);
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return script.toXContent(builder, params);
    }

    public static ScriptTransform parse(String watchId, XContentParser parser) throws IOException {
        try {
            Script script = Script.parse(parser);
            return new ScriptTransform(script);
        } catch (Script.ParseException pe) {
            throw new ScriptTransformException("could not parse [{}] transform for watch [{}]. failed to parse script", pe, TYPE, watchId);
        }
    }

    public static Builder builder(Script script) {
        return new Builder(script);
    }

    public static class Result extends Transform.Result {

        public Result(Payload payload) {
            super(TYPE, payload);
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        public static Result parse(String watchId, XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ScriptTransformException("could not parse [{}] transform result for watch [{}]. expected an object, but found [{}] instead", TYPE, watchId, token);
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.FIELD_NAME || !Field.PAYLOAD.match(parser.currentName())) {
                throw new ScriptTransformException("could not parse [{}] transform result for watch [{}]. expected a [{}] object, but found [{}] instead", TYPE, watchId, Field.PAYLOAD.getPreferredName(), token);
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ScriptTransformException("could not parse [{}] transform result for watch [{}]. expected a [{}] object, but found [{}] instead", TYPE, watchId, Field.PAYLOAD.getPreferredName(), token);
            }
            return new ScriptTransform.Result(new Payload.XContent(parser));
        }
    }

    public static class Builder implements Transform.Builder<ScriptTransform> {

        private final Script script;

        public Builder(Script script) {
            this.script = script;
        }

        @Override
        public ScriptTransform build() {
            return new ScriptTransform(script);
        }
    }
}
