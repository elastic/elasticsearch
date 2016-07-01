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

package org.elasticsearch.index.mapper.size;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.internal.EnabledAttributeMapper;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;

public final class MapperSizeTypeParser implements MetadataFieldMapper.TypeParser {
    @Override
    public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node,
                                                   ParserContext parserContext) throws MapperParsingException {
        AbstractSizeFieldMapper.Builder builder;
        if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha2)) {
            builder =
                new LegacySizeFieldMapper.Builder(parserContext.mapperService().fullName(AbstractSizeFieldMapper.NAME));
        } else {
            builder =
                new SizeFieldMapper.Builder(parserContext.mapperService().fullName(AbstractSizeFieldMapper.NAME));
        }
        for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(lenientNodeBooleanValue(fieldNode) ?
                    EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                iterator.remove();
            }
        }
        return builder;
    }

    @Override
    public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
        if (Version.indexCreated(indexSettings).before(Version.V_5_0_0_alpha2)) {
            return new LegacySizeFieldMapper(indexSettings, fieldType);
        }
        return new SizeFieldMapper(indexSettings, fieldType);
    }
}
