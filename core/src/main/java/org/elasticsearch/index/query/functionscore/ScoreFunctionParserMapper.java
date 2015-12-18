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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionParser;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScoreFunctionParserMapper {

    protected Map<String, ScoreFunctionParser<?>> functionParsers;

    @Inject
    public ScoreFunctionParserMapper(Set<ScoreFunctionParser> parsers, NamedWriteableRegistry namedWriteableRegistry) {
        Map<String, ScoreFunctionParser<?>> map = new HashMap<>();
        // built-in parsers
        addParser(new ScriptScoreFunctionParser(), map, namedWriteableRegistry);
        addParser(new GaussDecayFunctionParser(), map, namedWriteableRegistry);
        addParser(new LinearDecayFunctionParser(), map, namedWriteableRegistry);
        addParser(new ExponentialDecayFunctionParser(), map, namedWriteableRegistry);
        addParser(new RandomScoreFunctionParser(), map, namedWriteableRegistry);
        addParser(new FieldValueFactorFunctionParser(), map, namedWriteableRegistry);
        for (ScoreFunctionParser<?> scoreFunctionParser : parsers) {
            addParser(scoreFunctionParser, map, namedWriteableRegistry);
        }
        this.functionParsers = Collections.unmodifiableMap(map);
        //weight doesn't have its own parser, so every function supports it out of the box.
        //Can be a single function too when not associated to any other function, which is why it needs to be registered manually here.
        namedWriteableRegistry.registerPrototype(ScoreFunctionBuilder.class, new WeightBuilder());
    }

    public ScoreFunctionParser get(XContentLocation contentLocation, String parserName) {
        ScoreFunctionParser functionParser = get(parserName);
        if (functionParser == null) {
            throw new ParsingException(contentLocation, "No function with the name [" + parserName + "] is registered.");
        }
        return functionParser;
    }

    private ScoreFunctionParser get(String parserName) {
        return functionParsers.get(parserName);
    }

    private static void addParser(ScoreFunctionParser<? extends ScoreFunctionBuilder> scoreFunctionParser, Map<String, ScoreFunctionParser<?>> map, NamedWriteableRegistry namedWriteableRegistry) {
        for (String name : scoreFunctionParser.getNames()) {
            map.put(name, scoreFunctionParser);

        }
        @SuppressWarnings("unchecked") NamedWriteable<? extends ScoreFunctionBuilder> sfb = scoreFunctionParser.getBuilderPrototype();
        namedWriteableRegistry.registerPrototype(ScoreFunctionBuilder.class, sfb);
    }
}
