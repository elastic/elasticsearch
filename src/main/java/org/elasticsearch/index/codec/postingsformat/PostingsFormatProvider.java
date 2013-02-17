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
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 * A {@link PostingsFormatProvider} acts as a named container for specific
 * {@link PostingsFormat} implementations. Custom {@link PostingsFormat}
 * implementations can be exposed via
 * {@link CodecModule#addPostingFormat(String, Class)}
 * <p>
 * Each {@link PostingsFormatProvider} must provide a unique name for its
 * postings format in order to map the postings format to a specific field via
 * the mapping API. The name provided via {@link #name()} is used to lookup the
 * postings format in {@link PostingsFormatService#get(String)} and should be
 * identical to the values used in the field mappings.
 * </p>
 * <p>
 * {@link PostingsFormatProvider} instances are initialized with a
 * {@link Settings} subset below the
 * {@value PostingsFormatProvider#POSTINGS_FORMAT_SETTINGS_PREFIX} prefix and
 * will only see the sub-tree below their mapping name. For instance a postings
 * format <tt>ElasticFantastic</tt> will see settings below
 * <tt>index.codec.postings_format.elastic_fantastic</tt> given that the
 * postings format is exposed via
 * <tt>index.codec.postings_format.elastic_fantastic.type : "ElasticFantastic"</tt>.
 * </p>
 *
 * @see CodecModule
 */
public interface PostingsFormatProvider {
    public static final String POSTINGS_FORMAT_SETTINGS_PREFIX = "index.codec.postings_format";

    /**
     * A helper class to lookup {@link PostingsFormatProvider providers} by their unique {@link PostingsFormatProvider#name() name}
     */
    public static class Helper {

        /**
         * Looks up and creates {@link PostingsFormatProvider} for the given name.
         * <p>
         * The settings for the created {@link PostingsFormatProvider} is taken from the given index settings.
         * All settings with the {@value PostingsFormatProvider#POSTINGS_FORMAT_SETTINGS_PREFIX} prefix
         * and the formats name as the key are passed to the factory.
         * </p>
         *
         * @param indexSettings          the index settings to configure the postings format
         * @param name                   the name of the postings format to lookup
         * @param postingFormatFactories the factory mapping to lookup the {@link Factory} to create the {@link PostingsFormatProvider}
         * @return a fully configured {@link PostingsFormatProvider} for the given name.
         * @throws ElasticSearchIllegalArgumentException
         *          if the no {@link PostingsFormatProvider} for the given name parameter could be found.
         */
        public static PostingsFormatProvider lookup(@IndexSettings Settings indexSettings, String name, Map<String, Factory> postingFormatFactories) throws ElasticSearchIllegalArgumentException {
            Factory factory = postingFormatFactories.get(name);
            if (factory == null) {
                throw new ElasticSearchIllegalArgumentException("failed to find postings_format [" + name + "]");
            }
            Settings settings = indexSettings.getGroups(POSTINGS_FORMAT_SETTINGS_PREFIX).get(name);
            if (settings == null) {
                settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            return factory.create(name, settings);
        }
    }

    /**
     * Returns this providers {@link PostingsFormat} instance.
     */
    PostingsFormat get();

    /**
     * Returns the name of this providers {@link PostingsFormat}
     */
    String name();

    /**
     * A simple factory used to create {@link PostingsFormatProvider} used by
     * delegating providers like {@link BloomFilterLucenePostingsFormatProvider} or
     * {@link PulsingPostingsFormatProvider}. Those providers wrap other
     * postings formats to enrich their capabilities.
     */
    public interface Factory {
        PostingsFormatProvider create(String name, Settings settings);
    }
}
