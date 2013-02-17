/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
public class BloomFilterLucenePostingsFormatProvider extends AbstractPostingsFormatProvider {

    public static final class Defaults {
        public static final float MAX_SATURATION = 0.1f;
        public static final float SATURATION_LIMIT = 0.9f;
    }

    private final float desiredMaxSaturation;
    private final float saturationLimit;
    private final PostingsFormatProvider delegate;
    private final BloomFilteringPostingsFormat postingsFormat;

    @Inject
    public BloomFilterLucenePostingsFormatProvider(@IndexSettings Settings indexSettings, @Nullable Map<String, Factory> postingFormatFactories, @Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.desiredMaxSaturation = postingsFormatSettings.getAsFloat("desired_max_saturation", Defaults.MAX_SATURATION);
        this.saturationLimit = postingsFormatSettings.getAsFloat("saturation_limit", Defaults.SATURATION_LIMIT);
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

    public static class CustomBloomFilterFactory extends BloomFilterFactory {

        private final float desiredMaxSaturation;
        private final float saturationLimit;

        public CustomBloomFilterFactory() {
            this(Defaults.MAX_SATURATION, Defaults.SATURATION_LIMIT);
        }

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
