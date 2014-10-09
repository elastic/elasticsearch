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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.util.BloomFilter;

/**
 * This class represents the set of Elasticsearch "built-in"
 * {@link PostingsFormatProvider.Factory postings format factories}
 * <ul>
 * <li><b>bloom_default</b>: a postings format that uses a bloom filter to
 * improve term lookup performance. This is useful for primarily keys or fields
 * that are used as a delete key</li>
 * <li><b>default</b>: the default Elasticsearch postings format offering best
 * general purpose performance. This format is used if no postings format is
 * specified in the field mapping.</li>
 * <li><b>***</b>: other formats from Lucene core (e.g. Lucene41 as of Lucene 4.10)
 * </ul>
 */
public class PostingFormats {

    private static final ImmutableMap<String, PreBuiltPostingsFormatProvider.Factory> builtInPostingFormats;

    static {
        MapBuilder<String, PreBuiltPostingsFormatProvider.Factory> builtInPostingFormatsX = MapBuilder.newMapBuilder();
        // Add any PostingsFormat visible in the CLASSPATH (from Lucene core or via user's plugins).  Note that we no longer include
        // lucene codecs module since those codecs have no backwards compatibility between releases and can easily cause exceptions that
        // look like index corruption on upgrade:
        for (String luceneName : PostingsFormat.availablePostingsFormats()) {
            builtInPostingFormatsX.put(luceneName, new PreBuiltPostingsFormatProvider.Factory(PostingsFormat.forName(luceneName)));
        }
        final PostingsFormat defaultFormat = new Elasticsearch090PostingsFormat();
        builtInPostingFormatsX.put(PostingsFormatService.DEFAULT_FORMAT,
                                   new PreBuiltPostingsFormatProvider.Factory(PostingsFormatService.DEFAULT_FORMAT, defaultFormat));

        builtInPostingFormatsX.put("bloom_default", new PreBuiltPostingsFormatProvider.Factory("bloom_default", wrapInBloom(PostingsFormat.forName("Lucene41"))));

        builtInPostingFormats = builtInPostingFormatsX.immutableMap();
    }

    public static final boolean luceneBloomFilter = false;

    static PostingsFormat wrapInBloom(PostingsFormat delegate) {
        return new BloomFilterPostingsFormat(delegate, BloomFilter.Factory.DEFAULT);
    }

    public static PostingsFormatProvider.Factory getAsFactory(String name) {
        return builtInPostingFormats.get(name);
    }

    public static PostingsFormatProvider getAsProvider(String name) {
        final PreBuiltPostingsFormatProvider.Factory factory = builtInPostingFormats.get(name);
        return factory == null ? null : factory.get();
    }

    public static ImmutableCollection<PreBuiltPostingsFormatProvider.Factory> listFactories() {
        return builtInPostingFormats.values();
    }
}
