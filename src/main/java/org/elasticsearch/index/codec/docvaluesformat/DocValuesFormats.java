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

package org.elasticsearch.index.codec.docvaluesformat;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.common.collect.MapBuilder;

/**
 * This class represents the set of Elasticsearch "built-in"
 * {@link DocValuesFormatProvider.Factory doc values format factories}
 */
public class DocValuesFormats {

    private static final ImmutableMap<String, PreBuiltDocValuesFormatProvider.Factory> builtInDocValuesFormats;

    static {
        MapBuilder<String, PreBuiltDocValuesFormatProvider.Factory> builtInDocValuesFormatsX = MapBuilder.newMapBuilder();
        for (String name : DocValuesFormat.availableDocValuesFormats()) {
            builtInDocValuesFormatsX.put(name, new PreBuiltDocValuesFormatProvider.Factory(DocValuesFormat.forName(name)));
        }
        // LUCENE UPGRADE: update those DVF if necessary
        builtInDocValuesFormatsX.put(DocValuesFormatService.DEFAULT_FORMAT, new PreBuiltDocValuesFormatProvider.Factory(DocValuesFormatService.DEFAULT_FORMAT, DocValuesFormat.forName("Lucene49")));
        builtInDocValuesFormatsX.put("memory", new PreBuiltDocValuesFormatProvider.Factory("memory", DocValuesFormat.forName("Memory")));
        builtInDocValuesFormatsX.put("disk", new PreBuiltDocValuesFormatProvider.Factory("disk", DocValuesFormat.forName("Lucene49")));
        builtInDocValuesFormatsX.put("Disk", new PreBuiltDocValuesFormatProvider.Factory("Disk", DocValuesFormat.forName("Lucene49")));
        builtInDocValuesFormats = builtInDocValuesFormatsX.immutableMap();
    }

    public static DocValuesFormatProvider.Factory getAsFactory(String name) {
        return builtInDocValuesFormats.get(name);
    }

    public static DocValuesFormatProvider getAsProvider(String name) {
        final PreBuiltDocValuesFormatProvider.Factory factory = builtInDocValuesFormats.get(name);
        return factory == null ? null : factory.get();
    }

    public static ImmutableCollection<PreBuiltDocValuesFormatProvider.Factory> listFactories() {
        return builtInDocValuesFormats.values();
    }
}
