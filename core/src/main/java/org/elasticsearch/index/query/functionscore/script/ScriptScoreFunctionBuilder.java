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

package org.elasticsearch.index.query.functionscore.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * A function that uses a script to compute or influence the score of documents
 * that match with the inner query or filter.
 */
public class ScriptScoreFunctionBuilder extends ScoreFunctionBuilder<ScriptScoreFunctionBuilder> {

    private final Script script;

    public ScriptScoreFunctionBuilder(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("script must not be null");
        }
        this.script = script;
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
        return ScriptScoreFunctionParser.NAMES[0];
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
    }

    @Override
    protected ScriptScoreFunctionBuilder doReadFrom(StreamInput in) throws IOException {
        return new ScriptScoreFunctionBuilder(Script.readScript(in));
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
            SearchScript searchScript = context.getScriptService().search(context.lookup(), script, ScriptContext.Standard.SEARCH, Collections.emptyMap());
            return new ScriptScoreFunction(script, searchScript);
        } catch (Exception e) {
            throw new QueryShardException(context, "script_score: the script could not be loaded", e);
        }
    }
}
