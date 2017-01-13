package org.elasticsearch.script;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

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
    
    /**
     * Create a new ScriptException.
     * @param message A short and simple summary of what happened, such as "compile error". 
     *                Must not be {@code null}. 
     * @param cause The underlying cause of the exception. Must not be {@code null}.
     * @param scriptStack An implementation-specific "stacktrace" for the error in the script.
     *                Must not be {@code null}, but can be empty (though this should be avoided if possible).
     * @param script Identifier for which script failed. Must not be {@code null}.
     * @param lang Scripting engine language, such as "painless". Must not be {@code null}.
     * @throws NullPointerException if any parameters are {@code null}.
     */
    public ScriptException(String message, Throwable cause, List<String> scriptStack, String script, String lang) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
        this.scriptStack = Collections.unmodifiableList(Objects.requireNonNull(scriptStack));
        this.script = Objects.requireNonNull(script);
        this.lang = Objects.requireNonNull(lang);
    }

    /**
     * Deserializes a ScriptException from a {@code StreamInput}
     */
    public ScriptException(StreamInput in) throws IOException {
        super(in);
        scriptStack = Arrays.asList(in.readStringArray());
        script = in.readString();
        lang = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(scriptStack.toArray(new String[0]));
        out.writeString(script);
        out.writeString(lang);
    }
    
    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("script_stack", scriptStack);
        builder.field("script", script);
        builder.field("lang", lang);
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
     * Returns a JSON version of this exception for debugging.
     */
    public String toJsonString() {
        try {
            XContentBuilder json = XContentFactory.jsonBuilder().prettyPrint();
            json.startObject();
            toXContent(json, ToXContent.EMPTY_PARAMS);
            json.endObject();
            return json.string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
