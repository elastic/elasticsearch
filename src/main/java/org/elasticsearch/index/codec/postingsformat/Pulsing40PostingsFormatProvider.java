package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing40PostingsFormat;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 */
public class Pulsing40PostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final int freqCutOff;
    private final int minBlockSize;
    private final int maxBlockSize;
    private final Pulsing40PostingsFormat postingsFormat;

    @Inject
    public Pulsing40PostingsFormatProvider(@Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.freqCutOff = postingsFormatSettings.getAsInt("freq_cut_off", 1);
        this.minBlockSize = postingsFormatSettings.getAsInt("min_block_size", BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE);
        this.maxBlockSize = postingsFormatSettings.getAsInt("max_block_size", BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
        this.postingsFormat = new Pulsing40PostingsFormat(freqCutOff, minBlockSize, maxBlockSize);
    }

    public int freqCutOff() {
        return freqCutOff;
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
