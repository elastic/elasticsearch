/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing41PostingsFormat;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 * A {@link PostingsFormatProvider} for Lucenes {@link Pulsing41PostingsFormat}.
 * The pulsing implementation in-lines the posting lists for very low frequent
 * terms in the term dictionary. This is useful to improve lookup performance
 * for low-frequent terms. This postings format offers the following parameters:
 * <ul>
 * <li><tt>min_block_size</tt>: the minimum block size the default Lucene term
 * dictionary uses to encode on-disk blocks.</li>
 * 
 * <li><tt>max_block_size</tt>: the maximum block size the default Lucene term
 * dictionary uses to encode on-disk blocks.</li>
 * 
 * <li><tt>freq_cut_off</tt>: the document frequency cut off where pulsing
 * in-lines posting lists into the term dictionary. Terms with a document
 * frequency less or equal to the cutoff will be in-lined. The default is
 * <tt>1</tt></li>
 * </ul>
 */
// LUCENE UPGRADE: Check if type of field postingsFormat needs to be updated!
public class PulsingPostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final int freqCutOff;
    private final int minBlockSize;
    private final int maxBlockSize;
    private final Pulsing41PostingsFormat postingsFormat;

    @Inject
    public PulsingPostingsFormatProvider(@Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.freqCutOff = postingsFormatSettings.getAsInt("freq_cut_off", 1);
        this.minBlockSize = postingsFormatSettings.getAsInt("min_block_size", BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE);
        this.maxBlockSize = postingsFormatSettings.getAsInt("max_block_size", BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
        this.postingsFormat = new Pulsing41PostingsFormat(freqCutOff, minBlockSize, maxBlockSize);
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
