package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 */
public interface PostingsFormatProvider {

    public static class Helper {

        public static PostingsFormatProvider lookup(@IndexSettings Settings indexSettings, String name, Map<String, Factory> postingFormatFactories) throws ElasticSearchIllegalArgumentException {
            Factory factory = postingFormatFactories.get(name);
            if (factory == null) {
                throw new ElasticSearchIllegalArgumentException("failed to find postings_format [" + name + "]");
            }
            Settings settings = indexSettings.getGroups("index.codec.postings_format").get(name);
            if (settings == null) {
                settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            return factory.create(name, settings);
        }
    }

    PostingsFormat get();

    String name();

    public interface Factory {
        PostingsFormatProvider create(String name, Settings settings);
    }
}
