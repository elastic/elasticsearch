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
import org.elasticsearch.common.settings.Settings;

/**
 */
public class PreBuiltPostingsFormatProvider implements PostingsFormatProvider {

    public static final class Factory implements PostingsFormatProvider.Factory {

        private final PreBuiltPostingsFormatProvider provider;

        public Factory(PostingsFormat postingsFormat) {
            this(postingsFormat.getName(), postingsFormat);
        }

        public Factory(String name, PostingsFormat postingsFormat) {
            this.provider = new PreBuiltPostingsFormatProvider(name, postingsFormat);
        }

        public PostingsFormatProvider get() {
            return provider;
        }

        @Override
        public PostingsFormatProvider create(String name, Settings settings) {
            return provider;
        }

        public String name() {
            return provider.name();
        }
    }

    private final String name;
    private final PostingsFormat postingsFormat;

    public PreBuiltPostingsFormatProvider(PostingsFormat postingsFormat) {
        this(postingsFormat.getName(), postingsFormat);
    }

    public PreBuiltPostingsFormatProvider(String name, PostingsFormat postingsFormat) {
        if (postingsFormat == null) {
            throw new IllegalArgumentException("PostingsFormat must not be null");
        }
        this.name = name;
        this.postingsFormat = postingsFormat;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public PostingsFormat get() {
        return postingsFormat;
    }
}
