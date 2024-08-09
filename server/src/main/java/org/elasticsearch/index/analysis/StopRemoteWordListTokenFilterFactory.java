/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.lucene.analysis.StopFilter.makeStopSet;

public class StopRemoteWordListTokenFilterFactory extends AbstractTokenFilterFactory {

    private final SetOnce<CharArraySet> stopWordsHolder = new SetOnce<>();
    private final AtomicReference<Exception> asyncInitException = new AtomicReference<>();

    private final boolean ignoreCase;

    public StopRemoteWordListTokenFilterFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings,
        WordListsIndexService wordListsIndexService
    ) {
        super(name, settings);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        if (settings.get("enable_position_increments") != null) {
            throw new IllegalArgumentException("enable_position_increments is not supported anymore. Please fix your analysis chain");
        }

        Analysis.getWordListAsync(
            indexSettings.getIndex().getName(),
            env,
            settings,
            "stopwords_path",
            "stopwords",
            true,
            wordListsIndexService,
            new ActionListener<>() {
                @Override
                public void onResponse(List<String> wordList) {
                    stopWordsHolder.set(makeStopSet(wordList, ignoreCase));
                }

                @Override
                public void onFailure(Exception e) {
                    asyncInitException.set(e);
                }
            }
        );
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new StopRemoteWordListFilter(tokenStream, stopWordsHolder, asyncInitException);
    }

    public Set<?> stopWords() {
        return stopWordsHolder.get();
    }

    public boolean ignoreCase() {
        return ignoreCase;
    }

}
