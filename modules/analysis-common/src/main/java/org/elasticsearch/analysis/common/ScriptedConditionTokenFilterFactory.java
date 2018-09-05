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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.ReferringFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A factory for a conditional token filter that only applies child filters if the underlying token
 * matches an {@link AnalysisPredicateScript}
 */
public class ScriptedConditionTokenFilterFactory extends AbstractTokenFilterFactory implements ReferringFilterFactory {

    private final AnalysisPredicateScript.Factory factory;
    private final List<TokenFilterFactory> filters = new ArrayList<>();
    private final List<String> filterNames;

    ScriptedConditionTokenFilterFactory(IndexSettings indexSettings, String name,
                                               Settings settings, ScriptService scriptService) {
        super(indexSettings, name, settings);

        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        if (script.getType() != ScriptType.INLINE) {
            throw new IllegalArgumentException("Cannot use stored scripts in tokenfilter [" + name + "]");
        }
        this.factory = scriptService.compile(script, AnalysisPredicateScript.CONTEXT);

        this.filterNames = settings.getAsList("filter");
        if (this.filterNames.isEmpty()) {
            throw new IllegalArgumentException("Empty list of filters provided to tokenfilter [" + name + "]");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        Function<TokenStream, TokenStream> filter = in -> {
            for (TokenFilterFactory tff : filters) {
                in = tff.create(in);
            }
            return in;
        };
        AnalysisPredicateScript script = factory.newInstance();
        final AnalysisPredicateScript.Token token = new AnalysisPredicateScript.Token();
        return new ConditionalTokenFilter(tokenStream, filter) {

            CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
            PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
            OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
            TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
            KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);

            @Override
            protected boolean shouldFilter() {
                token.term = termAtt;
                token.posInc = posIncAtt.getPositionIncrement();
                token.pos += token.posInc;
                token.posLen = posLenAtt.getPositionLength();
                token.startOffset = offsetAtt.startOffset();
                token.endOffset = offsetAtt.endOffset();
                token.type = typeAtt.type();
                token.isKeyword = keywordAtt.isKeyword();
                return script.execute(token);
            }
        };
    }

    @Override
    public void setReferences(Map<String, TokenFilterFactory> factories) {
        for (String filter : filterNames) {
            TokenFilterFactory tff = factories.get(filter);
            if (tff == null) {
                throw new IllegalArgumentException("ScriptedConditionTokenFilter [" + name() +
                    "] refers to undefined token filter [" + filter + "]");
            }
            filters.add(tff);
        }
    }

}
