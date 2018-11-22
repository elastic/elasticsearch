package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;

/**
 * A TokenFilterFactory that may be used for normalization
 *
 * The default implementation delegates {@link #normalize(TokenStream)} to
 * {@link #create(TokenStream)}}.
 */
public interface NormalizingTokenFilterFactory extends TokenFilterFactory {

    @Override
    default TokenStream normalize(TokenStream tokenStream) {
        return create(tokenStream);
    }

}
