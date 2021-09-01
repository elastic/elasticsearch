/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.core.transform.TransformMessages;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ScriptConfig extends AbstractDiffable<ScriptConfig> implements Writeable, ToXContentObject {
    private static final Logger logger = LogManager.getLogger(ScriptConfig.class);

    // we store the in 2 formats: the raw format and the parsed format
    private final Map<String, Object> source;
    private final Script script;

    public ScriptConfig(final Map<String, Object> source, Script script) {
        this.source = source;
        this.script = script;
    }

    public ScriptConfig(final StreamInput in) throws IOException {
        source = in.readMap();
        script = in.readOptionalWriteable(Script::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(source);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(source);
        out.writeOptionalWriteable(script);
    }

    public Script getScript() {
        return script;
    }

    public static ScriptConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        // we need 2 passes, but the parser can not be cloned, so we parse 1st into a map and then re-parse that for syntax checking

        // remember the registry, needed for the 2nd pass
        NamedXContentRegistry registry = parser.getXContentRegistry();

        Map<String, Object> source = parser.mapOrdered();
        Script script = null;

        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(source);
            XContentParser sourceParser = XContentType.JSON.xContent()
                .createParser(registry, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(xContentBuilder).streamInput())
        ) {
            script = Script.parse(sourceParser);
        } catch (Exception e) {
            if (lenient) {
                logger.warn(TransformMessages.LOG_TRANSFORM_CONFIGURATION_BAD_SCRIPT, e);
            } else {
                throw e;
            }
        }

        return new ScriptConfig(source, script);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, script);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final ScriptConfig that = (ScriptConfig) other;

        return Objects.equals(this.source, that.source) && Objects.equals(this.script, that.script);
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (script == null) {
            validationException = addValidationError("script must not be null", validationException);
        }
        return validationException;
    }
}
