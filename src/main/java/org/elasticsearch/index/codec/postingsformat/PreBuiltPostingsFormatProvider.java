package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.common.settings.Settings;

/**
 */
public class PreBuiltPostingsFormatProvider implements PostingsFormatProvider {

    public static final class Factory implements PostingsFormatProvider.Factory {

        private final PreBuiltPostingsFormatProvider provider;

        public Factory(PostingsFormat postingsFormat) {
            this(postingsFormat.getName(), postingsFormat);
        }

        public Factory(String name, PostingsFormat postingsFormat) {
            this.provider = new PreBuiltPostingsFormatProvider(name, postingsFormat);
        }

        public PostingsFormatProvider get() {
            return provider;
        }

        @Override
        public PostingsFormatProvider create(String name, Settings settings) {
            return provider;
        }

        public String name() {
            return provider.name();
        }
    }

    private final String name;
    private final PostingsFormat postingsFormat;

    public PreBuiltPostingsFormatProvider(PostingsFormat postingsFormat) {
        this(postingsFormat.getName(), postingsFormat);
    }

    public PreBuiltPostingsFormatProvider(String name, PostingsFormat postingsFormat) {
        this.name = name;
        this.postingsFormat = postingsFormat;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public PostingsFormat get() {
        return postingsFormat;
    }
}
