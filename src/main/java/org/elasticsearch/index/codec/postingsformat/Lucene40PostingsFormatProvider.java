package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 */
public class Lucene40PostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final int minBlockSize;
    private final int maxBlockSize;
    private final Lucene40PostingsFormat postingsFormat;

    @Inject
    public Lucene40PostingsFormatProvider(@Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.minBlockSize = postingsFormatSettings.getAsInt("min_block_size", BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE);
        this.maxBlockSize = postingsFormatSettings.getAsInt("max_block_size", BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
        this.postingsFormat = new Lucene40PostingsFormat(minBlockSize, maxBlockSize);
    }

    public int minBlockSize() {
        return minBlockSize;
    }

    public int maxBlockSize() {
        return maxBlockSize;
    }

    @Override
    public PostingsFormat get() {
        return postingsFormat;
    }
}
