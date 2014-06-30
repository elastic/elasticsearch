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
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.util.BloomFilter;

/**
 * This class represents the set of Elasticsearch "build-in"
 * {@link PostingsFormatProvider.Factory postings format factories}
 * <ul>
 * <li><b>direct</b>: a postings format that uses disk-based storage but loads
 * its terms and postings directly into memory. Note this postings format is
 * very memory intensive and has certain limitation that don't allow segments to
 * grow beyond 2.1GB see {@link DirectPostingsFormat} for details.</li>
 * <p/>
 * <li><b>memory</b>: a postings format that stores its entire terms, postings,
 * positions and payloads in a finite state transducer. This format should only
 * be used for primary keys or with fields where each term is contained in a
 * very low number of documents.</li>
 * <p/>
 * <li><b>pulsing</b>: a postings format in-lines the posting lists for very low
 * frequent terms in the term dictionary. This is useful to improve lookup
 * performance for low-frequent terms.</li>
 * <p/>
 * <li><b>bloom_default</b>: a postings format that uses a bloom filter to
 * improve term lookup performance. This is useful for primarily keys or fields
 * that are used as a delete key</li>
 * <p/>
 * <li><b>bloom_pulsing</b>: a postings format that combines the advantages of
 * <b>bloom</b> and <b>pulsing</b> to further improve lookup performance</li>
 * <p/>
 * <li><b>default</b>: the default Elasticsearch postings format offering best
 * general purpose performance. This format is used if no postings format is
 * specified in the field mapping.</li>
 * </ul>
 */
public class PostingFormats {

    private static final ImmutableMap<String, PreBuiltPostingsFormatProvider.Factory> builtInPostingFormats;

    static {
        MapBuilder<String, PreBuiltPostingsFormatProvider.Factory> buildInPostingFormatsX = MapBuilder.newMapBuilder();
        // add defaults ones
        for (String luceneName : PostingsFormat.availablePostingsFormats()) {
            buildInPostingFormatsX.put(luceneName, new PreBuiltPostingsFormatProvider.Factory(PostingsFormat.forName(luceneName)));
        }
        final PostingsFormat defaultFormat = new Elasticsearch090PostingsFormat();
        buildInPostingFormatsX.put("direct", new PreBuiltPostingsFormatProvider.Factory("direct", PostingsFormat.forName("Direct")));
        buildInPostingFormatsX.put("memory", new PreBuiltPostingsFormatProvider.Factory("memory", PostingsFormat.forName("Memory")));
        // LUCENE UPGRADE: Need to change this to the relevant ones on a lucene upgrade
        buildInPostingFormatsX.put("pulsing", new PreBuiltPostingsFormatProvider.Factory("pulsing", PostingsFormat.forName("Pulsing41")));
        buildInPostingFormatsX.put(PostingsFormatService.DEFAULT_FORMAT, new PreBuiltPostingsFormatProvider.Factory(PostingsFormatService.DEFAULT_FORMAT, defaultFormat));

        buildInPostingFormatsX.put("bloom_pulsing", new PreBuiltPostingsFormatProvider.Factory("bloom_pulsing", wrapInBloom(PostingsFormat.forName("Pulsing41"))));
        buildInPostingFormatsX.put("bloom_default", new PreBuiltPostingsFormatProvider.Factory("bloom_default", wrapInBloom(PostingsFormat.forName("Lucene41"))));

        builtInPostingFormats = buildInPostingFormatsX.immutableMap();
    }

    public static final boolean luceneBloomFilter = false;

    static PostingsFormat wrapInBloom(PostingsFormat delegate) {
        if (luceneBloomFilter) {
            return new BloomFilteringPostingsFormat(delegate, new BloomFilterLucenePostingsFormatProvider.CustomBloomFilterFactory());
        }
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
