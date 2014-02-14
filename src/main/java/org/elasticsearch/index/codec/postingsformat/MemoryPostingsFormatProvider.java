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

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 * A {@link PostingsFormatProvider} for Lucenes {@link MemoryPostingsFormat}.
 * This postings format offers the following parameters:
 * <ul>
 * <li><tt>pack_fst</tt>: <code>true</code> iff the in memory structure should
 * be packed once its build. Packed will reduce the size for the data-structure
 * in memory but requires more memory during building. Default is <code>false</code></li>
 * 
 * <li><tt>acceptable_overhead_ratio</tt>: the compression overhead used to
 * compress internal structures. See {@link PackedInts} for details. Default is {@value PackedInts#DEFAULT}</li>
 * </ul>
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
        // TODO this should really be an ENUM?
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
