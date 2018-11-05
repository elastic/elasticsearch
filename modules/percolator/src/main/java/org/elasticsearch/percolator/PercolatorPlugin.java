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

package org.elasticsearch.percolator;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.fetch.FetchSubPhase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class PercolatorPlugin extends Plugin implements MapperPlugin, SearchPlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(PercolateQueryBuilder.NAME, PercolateQueryBuilder::new, PercolateQueryBuilder::fromXContent));
    }

    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return Arrays.asList(
            new PercolatorMatchedSlotSubFetchPhase(),
            new PercolatorHighlightSubFetchPhase(context.getHighlighters())
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(PercolatorFieldMapper.INDEX_MAP_UNMAPPED_FIELDS_AS_TEXT_SETTING);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(PercolatorFieldMapper.CONTENT_TYPE, new PercolatorFieldMapper.TypeParser());
    }

}
