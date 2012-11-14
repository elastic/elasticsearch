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
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 */
public interface PostingsFormatProvider {

    public static class Helper {

        public static PostingsFormatProvider lookup(@IndexSettings Settings indexSettings, String name, Map<String, Factory> postingFormatFactories) throws ElasticSearchIllegalArgumentException {
            Factory factory = postingFormatFactories.get(name);
            if (factory == null) {
                throw new ElasticSearchIllegalArgumentException("failed to find postings_format [" + name + "]");
            }
            Settings settings = indexSettings.getGroups("index.codec.postings_format").get(name);
            if (settings == null) {
                settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            return factory.create(name, settings);
        }
    }

    PostingsFormat get();

    String name();

    public interface Factory {
        PostingsFormatProvider create(String name, Settings settings);
    }
}
