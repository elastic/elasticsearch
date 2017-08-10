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

package org.elasticsearch.index.search;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ScaledFloatFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class QueryParserHelper {
    // Mapping types the "all-ish" query can be executed against
    private static final Set<String> ALLOWED_QUERY_MAPPER_TYPES;

    static {
        ALLOWED_QUERY_MAPPER_TYPES = new HashSet<>();
        ALLOWED_QUERY_MAPPER_TYPES.add(DateFieldMapper.CONTENT_TYPE);
        ALLOWED_QUERY_MAPPER_TYPES.add(IpFieldMapper.CONTENT_TYPE);
        ALLOWED_QUERY_MAPPER_TYPES.add(KeywordFieldMapper.CONTENT_TYPE);
        for (NumberFieldMapper.NumberType nt : NumberFieldMapper.NumberType.values()) {
            ALLOWED_QUERY_MAPPER_TYPES.add(nt.typeName());
        }
        ALLOWED_QUERY_MAPPER_TYPES.add(ScaledFloatFieldMapper.CONTENT_TYPE);
        ALLOWED_QUERY_MAPPER_TYPES.add(TextFieldMapper.CONTENT_TYPE);
    }

    private QueryParserHelper() {}

    public static FieldMapper getFieldMapper(MapperService mapperService, String field) {
        for (DocumentMapper mapper : mapperService.docMappers(true)) {
            FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper(field);
            if (fieldMapper != null) {
                return fieldMapper;
            }
        }
        return null;
    }

    public static Map<String, Float> resolveMappingFields(QueryShardContext context,
                                                          Map<String, Float> fieldsAndWeights) {
        return resolveMappingFields(context, fieldsAndWeights, false, null);
    }

    public static Map<String, Float> resolveMappingFields(QueryShardContext context,
                                                          Map<String, Float> fieldsAndWeights,
                                                          boolean quoted,
                                                          String quoteFieldSuffix) {
        Map<String, Float> resolvedFields = new HashMap<>();
        for (Map.Entry<String, Float> fieldEntry : fieldsAndWeights.entrySet()) {
            boolean allField = Regex.isMatchAllPattern(fieldEntry.getKey());
            boolean multiField = Regex.isSimpleMatchPattern(fieldEntry.getKey());
            float weight = fieldEntry.getValue() == null ? 1.0f : fieldEntry.getValue();
            Map<String, Float> fieldMap = resolveMappingField(context, fieldEntry.getKey(), weight,
                !multiField, !allField, quoted, quoteFieldSuffix);
            resolvedFields.putAll(fieldMap);
        }
        return resolvedFields;
    }

    public static Map<String, Float> resolveMappingField(QueryShardContext context, String fieldOrPattern, float weight,
                                                         boolean acceptAllTypes, boolean acceptMetadataField) {
        return resolveMappingField(context, fieldOrPattern, weight, acceptAllTypes, acceptMetadataField, false, null);
    }

    public static Map<String, Float> resolveMappingField(QueryShardContext context, String fieldOrPattern, float weight,
                                                         boolean acceptAllTypes, boolean acceptMetadataField,
                                                         boolean quoted, String quoteFieldSuffix) {
        Collection<String> allFields = context.simpleMatchToIndexNames(fieldOrPattern);
        Map<String, Float> fields = new HashMap<>();
        for (String fieldName : allFields) {
            if (quoted && quoteFieldSuffix != null && context.fieldMapper(fieldName + quoteFieldSuffix) != null) {
                fieldName = fieldName + quoteFieldSuffix;
            }
            FieldMapper mapper = getFieldMapper(context.getMapperService(), fieldName);
            if (mapper == null) {
                // Unmapped fields are not ignored
                fields.put(fieldOrPattern, weight);
                continue;
            }
            if (acceptMetadataField == false && mapper instanceof MetadataFieldMapper) {
                // Ignore metadata fields
                continue;
            }
            // Ignore fields that are not in the allowed mapper types. Some
            // types do not support term queries, and thus we cannot generate
            // a special query for them.
            String mappingType = mapper.fieldType().typeName();
            if (acceptAllTypes == false && ALLOWED_QUERY_MAPPER_TYPES.contains(mappingType) == false) {
                continue;
            }
            fields.put(fieldName, weight);
        }
        return fields;
    }
}
