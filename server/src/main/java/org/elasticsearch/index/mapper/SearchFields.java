/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.query.QueryShardException;

import java.util.Collections;
import java.util.Set;

/**
 * Wraps the {@link MapperService} and exposes a subset of its functionality to the search layer.
 * This is where runtime fields are also exposed to the search layer, hence it is important that {@link MapperService}
 * does not leak as it exposes the ability to iterate through field mappers etc. which are not runtime fields aware.
 */
public class SearchFields {
    private final MapperService mapperService;
    private boolean allowUnmappedFields;
    private boolean mapUnmappedFieldAsText;

    public SearchFields(MapperService mapperService) {
        this(mapperService, mapperService.getIndexSettings().isDefaultAllowUnmappedFields());
    }

    public SearchFields(SearchFields searchFields) {
        this(searchFields.mapperService);
    }

    //this is here only for testing: it allows for a null MapperService
    SearchFields(MapperService mapperService, boolean allowUnmappedFields) {
        this.mapperService = mapperService;
        this.allowUnmappedFields = allowUnmappedFields;
    }

    public void setAllowUnmappedFields(boolean allowUnmappedFields) {
        this.allowUnmappedFields = allowUnmappedFields;
    }

    public void setMapUnmappedFieldAsText(boolean mapUnmappedFieldAsText) {
        this.mapUnmappedFieldAsText = mapUnmappedFieldAsText;
    }

    public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
        return mapperService.documentMapper() == null ? null : mapperService.documentMapper().parse(source);
    }

    public FieldNameAnalyzer getFieldNameIndexAnalyzer() {
        DocumentMapper documentMapper = mapperService.documentMapper();
        return documentMapper == null ? null : documentMapper.mappers().indexAnalyzer();
    }

    public boolean hasNested() {
        return mapperService.hasNested();
    }

    public boolean hasMappings() {
        return mapperService.documentMapper() != null;
    }

    /**
     * Returns all the fields that match a given pattern. If prefixed with a
     * type then the fields will be returned with a type prefix.
     */
    public Set<String> simpleMatchToIndexNames(String pattern) {
        return mapperService.simpleMatchToFullName(pattern);
    }

    /**
     * Returns the {@link MappedFieldType} for the provided field name.
     * If the field is not mapped, the behaviour depends on the index.query.parse.allow_unmapped_fields setting, which defaults to true.
     * In case unmapped fields are allowed, null is returned when the field is not mapped.
     * In case unmapped fields are not allowed, either an exception is thrown or the field is automatically mapped as a text field.
     *
     * @throws QueryShardException if unmapped fields are not allowed and automatically mapping unmapped fields as text is disabled.
     * @see SearchFields#setAllowUnmappedFields(boolean)
     * @see SearchFields#setMapUnmappedFieldAsText(boolean)
     */
    public MappedFieldType fieldType(String name) {
        return failIfFieldMappingNotFound(name, mapperService.fieldType(name));
    }

    /**
     * Returns true if the field identified by the provided name is mapped, false otherwise
     */
    public boolean isFieldMapped(String name) {
        return mapperService.fieldType(name) != null;
    }

    //TODO this is scary here as it allows to look up mappers which are not aware of runtime fields
    public ObjectMapper getObjectMapper(String name) {
        return mapperService.getObjectMapper(name);
    }

    /**
     * Given a type (eg. long, string, ...), returns an anonymous field type that can be used for search operations.
     * Generally used to handle unmapped fields in the context of sorting.
     */
    public MappedFieldType buildAnonymousFieldType(String type) {
        final Mapper.TypeParser.ParserContext parserContext = mapperService.parserContext();
        Mapper.TypeParser typeParser = parserContext.typeParser(type);
        if (typeParser == null) {
            throw new IllegalArgumentException("No mapper found for type [" + type + "]");
        }
        final Mapper.Builder builder = typeParser.parse("__anonymous_" + type, Collections.emptyMap(), parserContext);
        final Mapper.BuilderContext builderContext = new Mapper.BuilderContext(mapperService.getIndexSettings().getSettings(),
            new ContentPath(1));
        Mapper mapper = builder.build(builderContext);
        if (mapper instanceof FieldMapper) {
            return ((FieldMapper) mapper).fieldType();
        }
        throw new IllegalArgumentException("Mapper for type [" + type + "] must be a leaf field");
    }

    protected boolean mapUnmappedFieldAsText() {
        return mapUnmappedFieldAsText;
    }

    protected boolean allowUnmappedFields() {
        return allowUnmappedFields;
    }

    MappedFieldType failIfFieldMappingNotFound(String name, MappedFieldType fieldMapping) {
        if (fieldMapping != null || allowUnmappedFields()) {
            return fieldMapping;
        } else if (mapUnmappedFieldAsText()) {
            TextFieldMapper.Builder builder
                = new TextFieldMapper.Builder(name, () -> mapperService.getIndexAnalyzers().getDefaultIndexAnalyzer());
            return builder.build(new Mapper.BuilderContext(mapperService.getIndexSettings().getSettings(),
                new ContentPath(1))).fieldType();
        } else {
            throw new IllegalArgumentException("No field mapping can be found for the field with name [" + name + "]");
        }
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return mapperService.getIndexAnalyzers();
    }

    public Analyzer getIndexAnalyzer() {
        return mapperService.indexAnalyzer();
    }

    public boolean sourceEnabled() {
        return mapperService.documentMapper().sourceMapper().enabled();
    }

    public Set<String> sourcePath(String fieldName) {
        return mapperService.sourcePath(fieldName);
    }

    public boolean isMetadataField(String field) {
        return mapperService.isMetadataField(field);
    }
}
