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
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing41PostingsFormat;
import org.elasticsearch.common.collect.MapBuilder;

/**
 * This class represents the set of Elasticsearch "build-in"
 * {@link PostingsFormatProvider.Factory postings format factories}
 * <ul>
 * <li><b>direct</b>: a postings format that uses disk-based storage but loads
 * its terms and postings directly into memory. Note this postings format is
 * very memory intensive and has certain limitation that don't allow segments to
 * grow beyond 2.1GB see {@link DirectPostingsFormat} for details.</li>
 * 
 * <li><b>memory</b>: a postings format that stores its entire terms, postings,
 * positions and payloads in a finite state transducer. This format should only
 * be used for primary keys or with fields where each term is contained in a
 * very low number of documents.</li>
 * 
 * <li><b>pulsing</b>: a postings format in-lines the posting lists for very low
 * frequent terms in the term dictionary. This is useful to improve lookup
 * performance for low-frequent terms.</li>
 * 
 * <li><b>bloom_default</b>: a postings format that uses a bloom filter to
 * improve term lookup performance. This is useful for primarily keys or fields
 * that are used as a delete key</li>
 * 
 * <li><b>bloom_pulsing</b>: a postings format that combines the advantages of
 * <b>bloom</b> and <b>pulsing</b> to further improve lookup performance</li>
 * 
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
        buildInPostingFormatsX.put("direct", new PreBuiltPostingsFormatProvider.Factory("direct", new DirectPostingsFormat()));
        buildInPostingFormatsX.put("memory", new PreBuiltPostingsFormatProvider.Factory("memory", new MemoryPostingsFormat()));
        // LUCENE UPGRADE: Need to change this to the relevant ones on a lucene upgrade
        buildInPostingFormatsX.put("pulsing", new PreBuiltPostingsFormatProvider.Factory("pulsing", new Pulsing41PostingsFormat()));
        buildInPostingFormatsX.put("bloom_pulsing", new PreBuiltPostingsFormatProvider.Factory("bloom_pulsing", new BloomFilteringPostingsFormat(new Pulsing41PostingsFormat(), new BloomFilterPostingsFormatProvider.CustomBloomFilterFactory())));
        buildInPostingFormatsX.put("default", new PreBuiltPostingsFormatProvider.Factory("default", new Lucene41PostingsFormat()));
        buildInPostingFormatsX.put("bloom_default", new PreBuiltPostingsFormatProvider.Factory("bloom_default", new BloomFilteringPostingsFormat(new Lucene41PostingsFormat(), new BloomFilterPostingsFormatProvider.CustomBloomFilterFactory())));

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
