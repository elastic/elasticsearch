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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing40PostingsFormat;
import org.elasticsearch.common.collect.MapBuilder;

/**
 */
public class PostingFormats {

    private static final ImmutableMap<String, PreBuiltPostingsFormatProvider.Factory> builtInPostingFormats;

    static {
        MapBuilder<String, PreBuiltPostingsFormatProvider.Factory> buildInPostingFormatsX = MapBuilder.newMapBuilder();
        // add defaults ones
        for (String luceneName : PostingsFormat.availablePostingsFormats()) {
            buildInPostingFormatsX.put(luceneName, new PreBuiltPostingsFormatProvider.Factory(PostingsFormat.forName(luceneName)));
        }
        buildInPostingFormatsX.put("direct", new PreBuiltPostingsFormatProvider.Factory("direct", new DirectPostingsFormat()));
        buildInPostingFormatsX.put("memory", new PreBuiltPostingsFormatProvider.Factory("memory", new MemoryPostingsFormat()));
        // LUCENE UPGRADE: Need to change this to the relevant ones on a lucene upgrade
        buildInPostingFormatsX.put("pulsing", new PreBuiltPostingsFormatProvider.Factory("pulsing", new Pulsing40PostingsFormat()));
        buildInPostingFormatsX.put("bloom_pulsing", new PreBuiltPostingsFormatProvider.Factory("bloom_pulsing", new BloomFilteringPostingsFormat(new Pulsing40PostingsFormat(), new BloomFilterPostingsFormatProvider.CustomBloomFilterFactory())));
        buildInPostingFormatsX.put("default", new PreBuiltPostingsFormatProvider.Factory("default", new Lucene40PostingsFormat()));
        buildInPostingFormatsX.put("bloom_default", new PreBuiltPostingsFormatProvider.Factory("bloom_default", new BloomFilteringPostingsFormat(new Lucene40PostingsFormat(), new BloomFilterPostingsFormatProvider.CustomBloomFilterFactory())));

        builtInPostingFormats = buildInPostingFormatsX.immutableMap();
    }

    public static PostingsFormatProvider.Factory getAsFactory(String name) {
        return builtInPostingFormats.get(name);
    }

    public static PostingsFormatProvider getAsProvider(String name) {
        return builtInPostingFormats.get(name).get();
    }

    public static ImmutableCollection<PreBuiltPostingsFormatProvider.Factory> listFactories() {
        return builtInPostingFormats.values();
    }
}
