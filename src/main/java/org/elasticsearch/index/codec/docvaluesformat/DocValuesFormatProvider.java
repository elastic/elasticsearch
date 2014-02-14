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

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 * A {@link DocValuesFormatProvider} acts as a named container for specific
 * {@link DocValuesFormat} implementations. Custom {@link DocValuesFormat}
 * implementations can be exposed via
 * {@link CodecModule#addDocValuesFormat(String, Class)}
 *
 * @see CodecModule
 */
public interface DocValuesFormatProvider {
    public static final String DOC_VALUES_FORMAT_SETTINGS_PREFIX = "index.codec.doc_values_format";

    /**
     * A helper class to lookup {@link DocValuesFormatProvider providers} by their unique {@link DocValuesFormatProvider#name() name}
     */
    public static class Helper {

        /**
         * Looks up and creates {@link DocValuesFormatProvider} for the given name.
         * <p>
         * The settings for the created {@link DocValuesFormatProvider} is taken from the given index settings.
         * All settings with the {@value DocValuesFormatProvider#POSTINGS_FORMAT_SETTINGS_PREFIX} prefix
         * and the formats name as the key are passed to the factory.
         * </p>
         *
         * @param indexSettings          the index settings to configure the postings format
         * @param name                   the name of the doc values format to lookup
         * @param docValuesFormatFactories the factory mapping to lookup the {@link Factory} to create the {@link DocValuesFormatProvider}
         * @return a fully configured {@link DocValuesFormatProvider} for the given name.
         * @throws org.elasticsearch.ElasticsearchIllegalArgumentException
         *          if the no {@link DocValuesFormatProvider} for the given name parameter could be found.
         */
        public static DocValuesFormatProvider lookup(@IndexSettings Settings indexSettings, String name, Map<String, Factory> docValuesFormatFactories) throws ElasticsearchIllegalArgumentException {
            Factory factory = docValuesFormatFactories.get(name);
            if (factory == null) {
                throw new ElasticsearchIllegalArgumentException("failed to find doc_values_format [" + name + "]");
            }
            Settings settings = indexSettings.getGroups(DOC_VALUES_FORMAT_SETTINGS_PREFIX).get(name);
            if (settings == null) {
                settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            return factory.create(name, settings);
        }
    }

    /**
     * Returns this providers {@link DocValuesFormat} instance.
     */
    DocValuesFormat get();

    /**
     * Returns the name of this providers {@link DocValuesFormat}
     */
    String name();

    /**
     * A simple factory used to create {@link DocValuesFormatProvider}.
     */
    public interface Factory {
        DocValuesFormatProvider create(String name, Settings settings);
    }
}
