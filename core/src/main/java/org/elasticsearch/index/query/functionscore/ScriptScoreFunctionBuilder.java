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

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A function that uses a script to compute or influence the score of documents
 * that match with the inner query or filter.
 */
public class ScriptScoreFunctionBuilder extends ScoreFunctionBuilder<ScriptScoreFunctionBuilder> {
    public static final String NAME = "script_score";
    public static final ParseField FUNCTION_NAME_FIELD = new ParseField(NAME);

    private final Script script;

    public ScriptScoreFunctionBuilder(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("script must not be null");
        }
        this.script = script;
    }

    /**
     * Read from a stream.
     */
    public ScriptScoreFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        script = new Script(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
    }

    public Script getScript() {
        return this.script;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(ScriptField.SCRIPT.getPreferredName(), script);
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(ScriptScoreFunctionBuilder functionBuilder) {
        return Objects.equals(this.script, functionBuilder.script);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.script);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) {
        try {
            SearchScript searchScript = context.getScriptService().search(context.lookup(), script, ScriptContext.Standard.SEARCH,
                    Collections.emptyMap(), context.getClusterState());
            return new ScriptScoreFunction(script, searchScript);
        } catch (Exception e) {
            throw new QueryShardException(context, "script_score: the script could not be loaded", e);
        }
    }

    public static ScriptScoreFunctionBuilder fromXContent(QueryParseContext parseContext)
            throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        Script script = null;
        Map<String, Object> vars = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, parseContext.getParseFieldMatcher());
                } else if ("params".equals(currentFieldName)) { // TODO remove in 3.0 (here to support old script APIs)
                    vars = parser.map();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (!scriptParameterParser.token(currentFieldName, token, parser, parseContext.getParseFieldMatcher())) {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (script == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                if (vars == null) {
                    vars = new HashMap<>();
                }
                script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), vars);
            }
        } else if (vars != null) {
            throw new ParsingException(parser.getTokenLocation(), "script params must be specified inside script object");
        }

        if (script == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " requires 'script' field");
        }

        return new ScriptScoreFunctionBuilder(script);
    }
}
