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


package org.elasticsearch.search.aggregations.bucket.significant.heuristics;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.script.*;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ScriptHeuristic extends SignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("script_heuristic");
    private final LongAccessor subsetSizeHolder;
    private final LongAccessor supersetSizeHolder;
    private final LongAccessor subsetDfHolder;
    private final LongAccessor supersetDfHolder;
    ExecutableScript script = null;
    String scriptLang;
    String scriptString;
    ScriptService.ScriptType scriptType;
    Map<String, Object> params;

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {
            return new ScriptHeuristic(null, in.readOptionalString(), in.readString(), ScriptService.ScriptType.readFrom(in), in.readMap());
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    public ScriptHeuristic(ExecutableScript searchScript, String scriptLang, String scriptString, ScriptService.ScriptType scriptType, Map<String, Object> params) {
        subsetSizeHolder = new LongAccessor();
        supersetSizeHolder = new LongAccessor();
        subsetDfHolder = new LongAccessor();
        supersetDfHolder = new LongAccessor();
        this.script = searchScript;
        if (script != null) {
            script.setNextVar("_subset_freq", subsetDfHolder);
            script.setNextVar("_subset_size", subsetSizeHolder);
            script.setNextVar("_superset_freq", supersetDfHolder);
            script.setNextVar("_superset_size", supersetSizeHolder);
        }
        this.scriptLang = scriptLang;
        this.scriptString = scriptString;
        this.scriptType = scriptType;
        this.params = params;


    }

    public void initialize(InternalAggregation.ReduceContext context) {
        script = context.scriptService().executable(new Script(scriptLang, scriptString, scriptType, params), ScriptContext.Standard.AGGS);
        script.setNextVar("_subset_freq", subsetDfHolder);
        script.setNextVar("_subset_size", subsetSizeHolder);
        script.setNextVar("_superset_freq", supersetDfHolder);
        script.setNextVar("_superset_size", supersetSizeHolder);
    }

    /**
     * Calculates score with a script
     *
     * @param subsetFreq   The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetFreq The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        if (script == null) {
            //In tests, wehn calling assertSearchResponse(..) the response is streamed one additional time with an arbitrary version, see assertVersionSerializable(..).
            // Now, for version before 1.5.0 the score is computed after streaming the response but for scripts the script does not exists yet.
            // assertSearchResponse() might therefore fail although there is no problem.
            // This should be replaced by an exception in 2.0.
            ESLoggerFactory.getLogger("script heuristic").warn("cannot compute score - script has not been initialized yet.");
            return 0;
        }
        subsetSizeHolder.value = subsetSize;
        supersetSizeHolder.value = supersetSize;
        subsetDfHolder.value = subsetFreq;
        supersetDfHolder.value = supersetFreq;
        return ((Number) script.run()).doubleValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        out.writeOptionalString(scriptLang);
        out.writeString(scriptString);
        ScriptService.ScriptType.writeTo(scriptType, out);
        out.writeMap(params);
    }

    public static class ScriptHeuristicParser implements SignificanceHeuristicParser {
        private final ScriptService scriptService;
        @Inject
        public ScriptHeuristicParser(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryParsingException {
            NAMES_FIELD.match(parser.currentName(), ParseField.EMPTY_FLAGS);
            String script = null;
            String scriptLang;
            XContentParser.Token token;
            Map<String, Object> params = new HashMap<>();
            String currentFieldName = null;
            ScriptService.ScriptType scriptType = ScriptService.ScriptType.INLINE;
            ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token.equals(XContentParser.Token.FIELD_NAME)) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("params".equals(currentFieldName)) {
                        params = parser.map();
                    } else {
                        throw new ElasticsearchParseException("unknown object " + currentFieldName + " in script_heuristic");
                    }
                } else if (!scriptParameterParser.token(currentFieldName, token, parser)) {
                    throw new ElasticsearchParseException("unknown field " + currentFieldName + " in script_heuristic");
                }
            }

            ScriptParameterParser.ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                script = scriptValue.script();
                scriptType = scriptValue.scriptType();
            }
            scriptLang = scriptParameterParser.lang();

            if (script == null) {
                throw new ElasticsearchParseException("No script found in script_heuristic");
            }
            ExecutableScript searchScript;
            try {
                searchScript = scriptService.executable(new Script(scriptLang, script, scriptType, params), ScriptContext.Standard.AGGS);
            } catch (Exception e) {
                throw new ElasticsearchParseException("The script [" + script + "] could not be loaded", e);
            }
            return new ScriptHeuristic(searchScript, scriptLang, script, scriptType, params);
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class ScriptHeuristicBuilder implements SignificanceHeuristicBuilder {

        private String script = null;
        private String lang = null;
        private Map<String, Object> params = null;
        private String scriptId;
        private String scriptFile;
        public ScriptHeuristicBuilder setScript(String script) {
            this.script = script;
            return this;
        }

        public ScriptHeuristicBuilder setScriptFile(String script) {
            this.scriptFile = script;
            return this;
        }

        public ScriptHeuristicBuilder setLang(String lang) {
            this.lang = lang;
            return this;
        }

        public ScriptHeuristicBuilder setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        public ScriptHeuristicBuilder setScriptId(String scriptId) {
            this.scriptId = scriptId;
            return this;
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject(STREAM.getName());
            if (script != null) {
                builder.field("script", script);
            }
            if (lang != null) {
                builder.field("lang", lang);
            }
            if (params != null) {
                builder.field("params", params);
            }
            if (scriptId != null) {
                builder.field("script_id", scriptId);
            }
            if (scriptFile != null) {
                builder.field("script_file", scriptFile);
            }
            builder.endObject();
        }

    }

    public final class LongAccessor extends Number {
        public long value;
        public int intValue() {
            return (int)value;
        }
        public long longValue() {
            return value;
        }

        @Override
        public float floatValue() {
            return (float)value;
        }

        @Override
        public double doubleValue() {
            return (double)value;
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }
    }
}

