/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Exception from a scripting engine.
 * <p>
 * A ScriptException has the following components:
 * <ul>
 *   <li>{@code message}: A short and simple summary of what happened, such as "compile error".
 *   <li>{@code cause}: The underlying cause of the exception.
 *   <li>{@code scriptStack}: An implementation-specific "stacktrace" for the error in the script.
 *   <li>{@code script}: Identifier for which script failed.
 *   <li>{@code lang}: Scripting engine language, such as "painless"
 * </ul>
 */
@SuppressWarnings("serial")
public class ScriptException extends ElasticsearchException {
    private final List<String> scriptStack;
    private final String script;
    private final String lang;
    private final Position pos;

    /**
     * Create a new ScriptException.
     * @param message A short and simple summary of what happened, such as "compile error".
     *                Must not be {@code null}.
     * @param cause The underlying cause of the exception. Must not be {@code null}.
     * @param scriptStack An implementation-specific "stacktrace" for the error in the script.
     *                Must not be {@code null}, but can be empty (though this should be avoided if possible).
     * @param script Identifier for which script failed. Must not be {@code null}.
     * @param lang Scripting engine language, such as "painless". Must not be {@code null}.
     * @param pos Position of error within script, may be {@code null}.
     * @throws NullPointerException if any parameters are {@code null} except pos.
     */
    public ScriptException(String message, Throwable cause, List<String> scriptStack, String script, String lang, Position pos) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
        this.scriptStack = Collections.unmodifiableList(Objects.requireNonNull(scriptStack));
        this.script = Objects.requireNonNull(script);
        this.lang = Objects.requireNonNull(lang);
        this.pos = pos;
    }

    /**
     * Create a new ScriptException with null Position.
     */
    public ScriptException(String message, Throwable cause, List<String> scriptStack, String script, String lang) {
        this(message, cause, scriptStack, script, lang, null);
    }

    /**
     * Deserializes a ScriptException from a {@code StreamInput}
     */
    public ScriptException(StreamInput in) throws IOException {
        super(in);
        scriptStack = Arrays.asList(in.readStringArray());
        script = in.readString();
        lang = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_7_0) && in.readBoolean()) {
            pos = new Position(in);
        } else {
            pos = null;
        }
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeStringCollection(scriptStack);
        out.writeString(script);
        out.writeString(lang);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_7_0)) {
            if (pos == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                pos.writeTo(out);
            }
        }
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("script_stack", scriptStack);
        builder.field("script", script);
        builder.field("lang", lang);
        if (pos != null) {
            pos.toXContent(builder, params);
        }
    }

    /**
     * Returns the stacktrace for the error in the script.
     * @return a read-only list of frames, which may be empty.
     */
    public List<String> getScriptStack() {
        return scriptStack;
    }

    /**
     * Returns the identifier for which script.
     * @return script's name or source text that identifies the script.
     */
    public String getScript() {
        return script;
    }

    /**
     * Returns the language of the script.
     * @return the {@code lang} parameter of the scripting engine.
     */
    public String getLang() {
        return lang;
    }

    /**
     * Returns the position of the error.
     */
    public Position getPos() {
        return pos;
    }

    /**
     * Returns a JSON version of this exception for debugging.
     */
    public String toJsonString() {
        try {
            XContentBuilder json = XContentFactory.jsonBuilder().prettyPrint();
            json.startObject();
            toXContent(json, ToXContent.EMPTY_PARAMS);
            json.endObject();
            return Strings.toString(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    public static class Position {
        public final int offset;
        public final int start;
        public final int end;

        public Position(int offset, int start, int end) {
            this.offset = offset;
            this.start = start;
            this.end = end;
        }

        Position(StreamInput in) throws IOException {
            offset = in.readInt();
            start = in.readInt();
            end = in.readInt();
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeInt(offset);
            out.writeInt(start);
            out.writeInt(end);
        }

        void toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("position");
            builder.field("offset", offset);
            builder.field("start", start);
            builder.field("end", end);
            builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Position position = (Position) o;
            return offset == position.offset && start == position.start && end == position.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, start, end);
        }
    }
}
