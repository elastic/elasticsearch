package org.elasticsearch.index.analysis;

import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.Reader;

public class IcuAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Normalizer2 normalizer;

    public IcuAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        String method = settings.get("method", "nfkc_cf");
        String mode = settings.get("mode");
        if (!"compose".equals(mode) && !"decompose".equals(mode)) {
            mode = "compose";
        }
        Normalizer2 normalizer = Normalizer2.getInstance(
            null, method, "compose".equals(mode) ? Normalizer2.Mode.COMPOSE : Normalizer2.Mode.DECOMPOSE);
        this.normalizer = IcuNormalizerTokenFilterFactory.wrapWithUnicodeSetFilter(normalizer, settings);
    }

    @Override
    public Analyzer get() {
        return new Analyzer() {

            @Override
            protected Reader initReader(String fieldName, Reader reader) {
                return new ICUNormalizer2CharFilter(reader, normalizer);
            }

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new ICUTokenizer();
                return new TokenStreamComponents(source, new ICUFoldingFilter(source));
            }
        };
    }
}
