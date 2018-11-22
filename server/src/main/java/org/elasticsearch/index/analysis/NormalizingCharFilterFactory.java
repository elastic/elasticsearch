package org.elasticsearch.index.analysis;

import java.io.Reader;

/**
 * A CharFilterFactory that also supports normalization
 *
 * The default implementation of {@link #normalize(Reader)} delegates to
 * {@link #create(Reader)}
 */
public interface NormalizingCharFilterFactory extends CharFilterFactory {

    @Override
    default Reader normalize(Reader reader) {
        return create(reader);
    }

}
