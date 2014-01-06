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

import com.google.common.base.Preconditions;
import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.common.settings.Settings;

/**
 * Pre-built format provider which accepts no configuration option.
 */
public class PreBuiltDocValuesFormatProvider implements DocValuesFormatProvider {

    public static final class Factory implements DocValuesFormatProvider.Factory {

        private final PreBuiltDocValuesFormatProvider provider;

        public Factory(DocValuesFormat docValuesFormat) {
            this(docValuesFormat.getName(), docValuesFormat);
        }

        public Factory(String name, DocValuesFormat docValuesFormat) {
            this.provider = new PreBuiltDocValuesFormatProvider(name, docValuesFormat);
        }

        public DocValuesFormatProvider get() {
            return provider;
        }

        @Override
        public DocValuesFormatProvider create(String name, Settings settings) {
            return provider;
        }

        public String name() {
            return provider.name();
        }
    }

    private final String name;
    private final DocValuesFormat docValuesFormat;

    public PreBuiltDocValuesFormatProvider(DocValuesFormat postingsFormat) {
        this(postingsFormat.getName(), postingsFormat);
    }

    public PreBuiltDocValuesFormatProvider(String name, DocValuesFormat postingsFormat) {
        Preconditions.checkNotNull(postingsFormat, "DocValuesFormat must not be null");
        this.name = name;
        this.docValuesFormat = postingsFormat;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DocValuesFormat get() {
        return docValuesFormat;
    }
}
