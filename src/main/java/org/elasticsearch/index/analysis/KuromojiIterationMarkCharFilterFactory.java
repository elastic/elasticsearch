package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.ja.JapaneseIterationMarkCharFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Reader;

public class KuromojiIterationMarkCharFilterFactory extends AbstractCharFilterFactory {

    private final boolean normalizeKanji;
    private final boolean normalizeKana;

    @Inject
    public KuromojiIterationMarkCharFilterFactory(Index index, @IndexSettings Settings indexSettings,
                                                  @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
        normalizeKanji = settings.getAsBoolean("normalize_kanji", JapaneseIterationMarkCharFilter.NORMALIZE_KANJI_DEFAULT);
        normalizeKana = settings.getAsBoolean("normalize_kana", JapaneseIterationMarkCharFilter.NORMALIZE_KANA_DEFAULT);
    }

    @Override
    public Reader create(Reader reader) {
        return new JapaneseIterationMarkCharFilter(reader, normalizeKanji, normalizeKana);
    }
}
