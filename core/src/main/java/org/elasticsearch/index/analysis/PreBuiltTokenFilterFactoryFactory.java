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

package org.elasticsearch.index.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.PreBuiltTokenFilters;

public class PreBuiltTokenFilterFactoryFactory implements TokenFilterFactoryFactory {

    private final TokenFilterFactory tokenFilterFactory;

    public PreBuiltTokenFilterFactoryFactory(TokenFilterFactory tokenFilterFactory) {
        this.tokenFilterFactory = tokenFilterFactory;
    }

    @Override
    public TokenFilterFactory create(String name, Settings settings) {
        Version indexVersion = Version.indexCreated(settings);
        if (!Version.CURRENT.equals(indexVersion)) {
            PreBuiltTokenFilters preBuiltTokenFilters = PreBuiltTokenFilters.getOrDefault(name, null);
            if (preBuiltTokenFilters != null) {
                return preBuiltTokenFilters.getTokenFilterFactory(indexVersion);
            }
        }
        return tokenFilterFactory;
    }
}
