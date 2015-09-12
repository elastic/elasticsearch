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
package org.elasticsearch.index.mapper.core;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.suggest.completion.AnalyzingCompletionLookupProvider;
import org.elasticsearch.search.suggest.context.ContextBuilder;
import org.elasticsearch.search.suggest.context.ContextMapping;
import org.junit.Before;

import java.util.SortedMap;
import java.util.TreeMap;

public class CompletionFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        CompletionFieldMapper.CompletionFieldType ft = new CompletionFieldMapper.CompletionFieldType();
        ft.setProvider(new AnalyzingCompletionLookupProvider(true, false, true, false));
        return ft;
    }

    @Before
    public void setupProperties() {
        addModifier(new Modifier("preserve_separators", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                CompletionFieldMapper.CompletionFieldType cft = (CompletionFieldMapper.CompletionFieldType)ft;
                cft.setProvider(new AnalyzingCompletionLookupProvider(false, false, true, false));
            }
        });
        addModifier(new Modifier("preserve_position_increments", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                CompletionFieldMapper.CompletionFieldType cft = (CompletionFieldMapper.CompletionFieldType)ft;
                cft.setProvider(new AnalyzingCompletionLookupProvider(true, false, false, false));
            }
        });
        addModifier(new Modifier("payload", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                CompletionFieldMapper.CompletionFieldType cft = (CompletionFieldMapper.CompletionFieldType)ft;
                cft.setProvider(new AnalyzingCompletionLookupProvider(true, false, true, true));
            }
        });
        addModifier(new Modifier("context_mapping", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                CompletionFieldMapper.CompletionFieldType cft = (CompletionFieldMapper.CompletionFieldType)ft;
                SortedMap<String, ContextMapping> contextMapping = new TreeMap<>();
                contextMapping.put("foo", ContextBuilder.location("foo").build());
                cft.setContextMapping(contextMapping);
            }
        });
    }
}
