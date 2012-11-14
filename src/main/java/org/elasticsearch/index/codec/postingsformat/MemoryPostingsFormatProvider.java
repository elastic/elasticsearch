package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 */
public class MemoryPostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final boolean packFst;
    private final float acceptableOverheadRatio;
    private final MemoryPostingsFormat postingsFormat;

    @Inject
    public MemoryPostingsFormatProvider(@Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.packFst = postingsFormatSettings.getAsBoolean("pack_fst", false);
        this.acceptableOverheadRatio = postingsFormatSettings.getAsFloat("acceptable_overhead_ratio", PackedInts.DEFAULT);
        this.postingsFormat = new MemoryPostingsFormat(packFst, acceptableOverheadRatio);
    }

    public boolean packFst() {
        return packFst;
    }

    public float acceptableOverheadRatio() {
        return acceptableOverheadRatio;
    }

    @Override
    public PostingsFormat get() {
        return postingsFormat;
    }
}
