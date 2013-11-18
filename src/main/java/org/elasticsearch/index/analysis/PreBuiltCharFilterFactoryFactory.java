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

package org.elasticsearch.index.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.PreBuiltCharFilters;

import java.util.Locale;

public class PreBuiltCharFilterFactoryFactory implements CharFilterFactoryFactory {

    private final CharFilterFactory charFilterFactory;

    public PreBuiltCharFilterFactoryFactory(CharFilterFactory charFilterFactory) {
        this.charFilterFactory = charFilterFactory;
    }

    @Override
    public CharFilterFactory create(String name, Settings settings) {
        Version indexVersion = settings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
        if (!Version.CURRENT.equals(indexVersion)) {
            return PreBuiltCharFilters.valueOf(name.toUpperCase(Locale.ROOT)).getCharFilterFactory(indexVersion);
        }

        return charFilterFactory;
    }
}