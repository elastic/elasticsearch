/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.util.concurrent.ConcurrentCollections;

import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;

/**
 * A node level registry of analyzers, to be reused by different indices which use default analyzers.
 *
 * @author kimchy (shay.banon)
 */
public class IndicesAnalysisService extends AbstractComponent {

    private final Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories = ConcurrentCollections.newConcurrentMap();

    public IndicesAnalysisService() {
        super(EMPTY_SETTINGS);
    }

    @Inject public IndicesAnalysisService(Settings settings) {
        super(settings);

        analyzerProviderFactories.put("standard", new PreBuiltAnalyzerProviderFactory("standard", AnalyzerScope.INDICES, new StandardAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("keyword", new PreBuiltAnalyzerProviderFactory("keyword", AnalyzerScope.INDICES, new KeywordAnalyzer()));
        analyzerProviderFactories.put("stop", new PreBuiltAnalyzerProviderFactory("stop", AnalyzerScope.INDICES, new StopAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("whitespace", new PreBuiltAnalyzerProviderFactory("whitespace", AnalyzerScope.INDICES, new WhitespaceAnalyzer()));
        analyzerProviderFactories.put("simple", new PreBuiltAnalyzerProviderFactory("simple", AnalyzerScope.INDICES, new SimpleAnalyzer()));

        // extended ones
        analyzerProviderFactories.put("arabic", new PreBuiltAnalyzerProviderFactory("arabic", AnalyzerScope.INDICES, new ArabicAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("brazilian", new PreBuiltAnalyzerProviderFactory("brazilian", AnalyzerScope.INDICES, new BrazilianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("chinese", new PreBuiltAnalyzerProviderFactory("chinese", AnalyzerScope.INDICES, new ChineseAnalyzer()));
        analyzerProviderFactories.put("cjk", new PreBuiltAnalyzerProviderFactory("cjk", AnalyzerScope.INDICES, new ChineseAnalyzer()));
        analyzerProviderFactories.put("czech", new PreBuiltAnalyzerProviderFactory("czech", AnalyzerScope.INDICES, new CzechAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("dutch", new PreBuiltAnalyzerProviderFactory("dutch", AnalyzerScope.INDICES, new DutchAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("french", new PreBuiltAnalyzerProviderFactory("french", AnalyzerScope.INDICES, new FrenchAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("german", new PreBuiltAnalyzerProviderFactory("german", AnalyzerScope.INDICES, new GermanAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("greek", new PreBuiltAnalyzerProviderFactory("greek", AnalyzerScope.INDICES, new GreekAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("persian", new PreBuiltAnalyzerProviderFactory("persian", AnalyzerScope.INDICES, new PersianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("russian", new PreBuiltAnalyzerProviderFactory("russian", AnalyzerScope.INDICES, new RussianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("thai", new PreBuiltAnalyzerProviderFactory("thai", AnalyzerScope.INDICES, new ThaiAnalyzer(Lucene.ANALYZER_VERSION)));
    }

    public PreBuiltAnalyzerProviderFactory analyzerProviderFactory(String name) {
        return analyzerProviderFactories.get(name);
    }

    public boolean hasAnalyzer(String name) {
        return analyzer(name) != null;
    }

    public Analyzer analyzer(String name) {
        PreBuiltAnalyzerProviderFactory analyzerProviderFactory = analyzerProviderFactory(name);
        if (analyzerProviderFactory == null) {
            return null;
        }
        return analyzerProviderFactory.analyzer();
    }

    public void close() {
        for (PreBuiltAnalyzerProviderFactory analyzerProviderFactory : analyzerProviderFactories.values()) {
            analyzerProviderFactory.analyzer().close();
        }
    }
}
