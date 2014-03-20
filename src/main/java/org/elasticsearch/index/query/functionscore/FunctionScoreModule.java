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

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.factor.FactorParser;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionParser;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionParser;

import java.util.List;

/**
 *
 */
public class FunctionScoreModule extends AbstractModule {

    private List<Class<? extends ScoreFunctionParser>> parsers = Lists.newArrayList();

    public FunctionScoreModule() {
        registerParser(FactorParser.class);
        registerParser(ScriptScoreFunctionParser.class);
        registerParser(GaussDecayFunctionParser.class);
        registerParser(LinearDecayFunctionParser.class);
        registerParser(ExponentialDecayFunctionParser.class);
        registerParser(RandomScoreFunctionParser.class);
        registerParser(FieldValueFactorFunctionParser.class);
    }

    public void registerParser(Class<? extends ScoreFunctionParser> parser) {
        parsers.add(parser);
    }

    @Override
    protected void configure() {
        Multibinder<ScoreFunctionParser> parserMapBinder = Multibinder.newSetBinder(binder(), ScoreFunctionParser.class);
        for (Class<? extends ScoreFunctionParser> clazz : parsers) {
            parserMapBinder.addBinding().to(clazz);
        }
        bind(ScoreFunctionParserMapper.class);
    }
}
