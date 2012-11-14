package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 */
public class DirectPostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final int minSkipCount;
    private final int lowFreqCutoff;
    private final DirectPostingsFormat postingsFormat;

    @Inject
    public DirectPostingsFormatProvider(@Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.minSkipCount = postingsFormatSettings.getAsInt("min_skip_count", 8); // See DirectPostingsFormat#DEFAULT_MIN_SKIP_COUNT
        this.lowFreqCutoff = postingsFormatSettings.getAsInt("low_freq_cutoff", 32); // See DirectPostingsFormat#DEFAULT_LOW_FREQ_CUTOFF
        this.postingsFormat = new DirectPostingsFormat(minSkipCount, lowFreqCutoff);
    }

    public int minSkipCount() {
        return minSkipCount;
    }

    public int lowFreqCutoff() {
        return lowFreqCutoff;
    }

    @Override
    public PostingsFormat get() {
        return postingsFormat;
    }
}
