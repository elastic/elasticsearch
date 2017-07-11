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
package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.junit.Before;

import java.util.Arrays;

public class CompletionFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new CompletionFieldMapper.CompletionFieldType();
    }

    @Before
    public void setupProperties() {
        addModifier(new Modifier("preserve_separators", false) {
            @Override
            public void modify(MappedFieldType ft) {
                CompletionFieldMapper.CompletionFieldType cft = (CompletionFieldMapper.CompletionFieldType)ft;
                cft.setPreserveSep(false);
            }
        });
        addModifier(new Modifier("preserve_position_increments", false) {
            @Override
            public void modify(MappedFieldType ft) {
                CompletionFieldMapper.CompletionFieldType cft = (CompletionFieldMapper.CompletionFieldType)ft;
                cft.setPreservePositionIncrements(false);
            }
        });
        addModifier(new Modifier("context_mappings", false) {
            @Override
            public void modify(MappedFieldType ft) {
                CompletionFieldMapper.CompletionFieldType cft = (CompletionFieldMapper.CompletionFieldType)ft;
                ContextMappings contextMappings = new ContextMappings(Arrays.asList(ContextBuilder.category("foo").build(), ContextBuilder.geo("geo").build()));
                cft.setContextMappings(contextMappings);
            }
        });
    }
}
