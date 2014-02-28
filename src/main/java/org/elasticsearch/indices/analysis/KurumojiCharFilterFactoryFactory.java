/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
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

package org.elasticsearch.indices.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.PreBuiltCharFilterFactoryFactory;

public class KurumojiCharFilterFactoryFactory extends PreBuiltCharFilterFactoryFactory {
    private final CharFilterFactory charFilterFactory;

    public KurumojiCharFilterFactoryFactory(CharFilterFactory charFilterFactory) {
        super(charFilterFactory);
        this.charFilterFactory = charFilterFactory;
    }

    @Override
    public CharFilterFactory create(String name, Settings settings) {
        return charFilterFactory;
    }
}
