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
