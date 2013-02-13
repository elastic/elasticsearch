package org.elasticsearch.index.analysis;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;


/**
 * Only works for tokenizers with a constructor taking (only) a TokenStream as argument.
 */
public class ClassTokenFilterFactory extends AbstractTokenFilterFactory {
    private static final Map<String, Constructor> TOKEN_STREAM_CACHE = new ConcurrentHashMap<String, Constructor>();
    private String clazz;

    @Inject
    public ClassTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.clazz = settings.get("class");
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        try {
            if (TOKEN_STREAM_CACHE.containsKey(clazz)) {
                return (TokenStream) TOKEN_STREAM_CACHE.get(clazz).newInstance(tokenStream);
            }
            Constructor declaredConstructor = Class.forName(clazz).getDeclaredConstructor(TokenStream.class);
            TOKEN_STREAM_CACHE.put(clazz, declaredConstructor);
            return (TokenStream) declaredConstructor.newInstance(tokenStream);
        } catch (Exception e) {
            throw new ElasticSearchIllegalArgumentException("Unable to create TokenStream using class: " + clazz);
        }
    }

}
