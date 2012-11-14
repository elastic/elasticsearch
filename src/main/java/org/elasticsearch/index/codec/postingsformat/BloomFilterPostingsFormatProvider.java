package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.bloom.BloomFilterFactory;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 */
public class BloomFilterPostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final float desiredMaxSaturation;
    private final float saturationLimit;
    private final PostingsFormatProvider delegate;
    private final BloomFilteringPostingsFormat postingsFormat;

    @Inject
    public BloomFilterPostingsFormatProvider(@IndexSettings Settings indexSettings, @Nullable Map<String, Factory> postingFormatFactories, @Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.desiredMaxSaturation = postingsFormatSettings.getAsFloat("desired_max_saturation", 0.1f);
        this.saturationLimit = postingsFormatSettings.getAsFloat("saturation_limit", 0.9f);
        this.delegate = Helper.lookup(indexSettings, postingsFormatSettings.get("delegate"), postingFormatFactories);
        this.postingsFormat = new BloomFilteringPostingsFormat(
                delegate.get(),
                new CustomBloomFilterFactory(desiredMaxSaturation, saturationLimit)
        );
    }

    public float desiredMaxSaturation() {
        return desiredMaxSaturation;
    }

    public float saturationLimit() {
        return saturationLimit;
    }

    public PostingsFormatProvider delegate() {
        return delegate;
    }

    @Override
    public PostingsFormat get() {
        return postingsFormat;
    }

    static class CustomBloomFilterFactory extends BloomFilterFactory {

        private final float desiredMaxSaturation;
        private final float saturationLimit;

        CustomBloomFilterFactory(float desiredMaxSaturation, float saturationLimit) {
            this.desiredMaxSaturation = desiredMaxSaturation;
            this.saturationLimit = saturationLimit;
        }

        @Override
        public FuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
            //Assume all of the docs have a unique term (e.g. a primary key) and we hope to maintain a set with desiredMaxSaturation% of bits set
            return FuzzySet.createSetBasedOnQuality(state.segmentInfo.getDocCount(), desiredMaxSaturation);
        }

        @Override
        public boolean isSaturated(FuzzySet bloomFilter, FieldInfo fieldInfo) {
            // Don't bother saving bitsets if > saturationLimit % of bits are set - we don't want to
            // throw any more memory at this problem.
            return bloomFilter.getSaturation() > saturationLimit;
        }
    }
}
