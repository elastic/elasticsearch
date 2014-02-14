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
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 * The default postingsformat, maps to {@link Lucene41PostingsFormat}.
 * <ul>
 * <li><tt>min_block_size</tt>: the minimum block size the default Lucene term
 * dictionary uses to encode on-disk blocks.</li>
 * 
 * <li><tt>max_block_size</tt>: the maximum block size the default Lucene term
 * dictionary uses to encode on-disk blocks.</li>
 * </ul>
 */
// LUCENE UPGRADE: Check if type of field postingsFormat needs to be updated!
public class DefaultPostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final int minBlockSize;
    private final int maxBlockSize;
    private final Lucene41PostingsFormat postingsFormat;

    @Inject
    public DefaultPostingsFormatProvider(@Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.minBlockSize = postingsFormatSettings.getAsInt("min_block_size", BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE);
        this.maxBlockSize = postingsFormatSettings.getAsInt("max_block_size", BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
        this.postingsFormat = new Lucene41PostingsFormat(minBlockSize, maxBlockSize);
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
