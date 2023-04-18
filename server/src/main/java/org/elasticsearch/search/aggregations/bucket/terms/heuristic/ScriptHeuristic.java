/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.SignificantTermsHeuristicScoreScript;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ScriptHeuristic extends SignificanceHeuristic {
    public static final String NAME = "script_heuristic";
    public static final ConstructingObjectParser<ScriptHeuristic, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new ScriptHeuristic((Script) args[0])
    );
    static {
        Script.declareScript(PARSER, constructorArg());
    }

    private final Script script;

    // This class holds an executable form of the script with private variables ready for execution
    // on a single search thread.
    static class ExecutableScriptHeuristic extends ScriptHeuristic {
        private final LongAccessor subsetSizeHolder;
        private final LongAccessor supersetSizeHolder;
        private final LongAccessor subsetDfHolder;
        private final LongAccessor supersetDfHolder;
        private final SignificantTermsHeuristicScoreScript executableScript;
        private final Map<String, Object> params = new HashMap<>();

        ExecutableScriptHeuristic(Script script, SignificantTermsHeuristicScoreScript executableScript) {
            super(script);
            subsetSizeHolder = new LongAccessor();
            supersetSizeHolder = new LongAccessor();
            subsetDfHolder = new LongAccessor();
            supersetDfHolder = new LongAccessor();
            this.executableScript = executableScript;
            params.putAll(script.getParams());
            params.put("_subset_freq", subsetDfHolder);
            params.put("_subset_size", subsetSizeHolder);
            params.put("_superset_freq", supersetDfHolder);
            params.put("_superset_size", supersetSizeHolder);
        }

        @Override
        public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
            subsetSizeHolder.value = subsetSize;
            supersetSizeHolder.value = supersetSize;
            subsetDfHolder.value = subsetFreq;
            supersetDfHolder.value = supersetFreq;
            return executableScript.execute(params);
        }
    }

    public ScriptHeuristic(Script script) {
        this.script = script;
    }

    /**
     * Read from a stream.
     */
    public ScriptHeuristic(StreamInput in) throws IOException {
        this(new Script(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        script.writeTo(out);
    }

    @Override
    public SignificanceHeuristic rewrite(AggregationReduceContext context) {
        SignificantTermsHeuristicScoreScript.Factory factory = context.scriptService()
            .compile(script, SignificantTermsHeuristicScoreScript.CONTEXT);
        return new ExecutableScriptHeuristic(script, factory.newInstance());
    }

    @Override
    public SignificanceHeuristic rewrite(AggregationContext context) {
        SignificantTermsHeuristicScoreScript.Factory compiledScript = context.compile(script, SignificantTermsHeuristicScoreScript.CONTEXT);
        return new ExecutableScriptHeuristic(script, compiledScript.newInstance());
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
        throw new UnsupportedOperationException(
            "This scoring heuristic must have 'rewrite' called on it to provide a version ready " + "for use"
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject(NAME);
        builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName());
        script.toXContent(builder, builderParams);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(script);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ScriptHeuristic other = (ScriptHeuristic) obj;
        return Objects.equals(script, other.script);
    }

    public static final class LongAccessor extends Number {
        public long value;

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public long longValue() {
            return value;
        }

        @Override
        public float floatValue() {
            return value;
        }

        @Override
        public double doubleValue() {
            return value;
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }
    }
}
