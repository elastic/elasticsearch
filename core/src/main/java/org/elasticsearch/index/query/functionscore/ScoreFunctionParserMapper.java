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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionParser;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionParser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScoreFunctionParserMapper {

    protected Map<String, ScoreFunctionParser> functionParsers;

    @Inject
    public ScoreFunctionParserMapper(Set<ScoreFunctionParser> parsers) {
        Map<String, ScoreFunctionParser> map = new HashMap<>();
        // built-in parsers
        addParser(new ScriptScoreFunctionParser(), map);
        addParser(new GaussDecayFunctionParser(), map);
        addParser(new LinearDecayFunctionParser(), map);
        addParser(new ExponentialDecayFunctionParser(), map);
        addParser(new RandomScoreFunctionParser(), map);
        addParser(new FieldValueFactorFunctionParser(), map);
        for (ScoreFunctionParser scoreFunctionParser : parsers) {
            addParser(scoreFunctionParser, map);
        }
        this.functionParsers = Collections.unmodifiableMap(map);
    }

    public ScoreFunctionParser get(QueryParseContext parseContext, String parserName) {
        ScoreFunctionParser functionParser = get(parserName);
        if (functionParser == null) {
            throw new QueryParsingException(parseContext, "No function with the name [" + parserName + "] is registered.");
        }
        return functionParser;
    }

    private ScoreFunctionParser get(String parserName) {
        return functionParsers.get(parserName);
    }

    private void addParser(ScoreFunctionParser scoreFunctionParser, Map<String, ScoreFunctionParser> map) {
        for (String name : scoreFunctionParser.getNames()) {
            map.put(name, scoreFunctionParser);
        }
    }

}
