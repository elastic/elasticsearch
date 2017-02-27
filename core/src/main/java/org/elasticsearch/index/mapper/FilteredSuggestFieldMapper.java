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

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.suggest.document.Completion50PostingsFormat;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.search.suggest.document.RegexCompletionQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.NumberType;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.search.suggest.filteredsuggest.filter.FilterMappings;
import org.elasticsearch.search.suggest.filteredsuggest.filter.FilteredSuggestFilterValues;
import org.elasticsearch.search.suggest.filteredsuggest.filter.FilteredSuggestFilterMapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilteredSuggestFieldMapper extends BaseSuggestFieldMapper {

    public static final String CONTENT_TYPE = "filteredsuggest";

    public static class Defaults {
        public static final CompletionFieldType FIELD_TYPE = new CompletionFieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

        public static final boolean DEFAULT_PRESERVE_SEPARATORS = true;
        public static final boolean DEFAULT_POSITION_INCREMENTS = true;
        public static final int DEFAULT_MAX_INPUT_LENGTH = 50;
    }

    public static class Fields {
        // Mapping field names
        public static final ParseField ANALYZER = new ParseField("analyzer");
        public static final ParseField SEARCH_ANALYZER = new ParseField("search_analyzer");
        public static final ParseField PRESERVE_SEPARATORS = new ParseField("preserve_separators");
        public static final ParseField PRESERVE_POSITION_INCREMENTS = new ParseField("preserve_position_increments");
        public static final ParseField TYPE = new ParseField("type");
        public static final ParseField FILTERS = new ParseField("filters");
        public static final ParseField MAX_INPUT_LENGTH = new ParseField("max_input_length", "max_input_len");
        public static final ParseField INPUT_FIELDS = new ParseField("input_fields");
        public static final ParseField PARENT_PATH = new ParseField("parent_path");

        // Content field names
        public static final String CONTENT_FIELD_NAME_PARENT_PATH = "parent_path";
        public static final String CONTENT_FIELD_NAME_INPUT = "input";
        public static final String CONTENT_FIELD_NAME_WEIGHT = "weight";
        public static final String CONTENT_FIELD_NAME_INPUT_FIELDS = "input_fields";
        public static final String CONTENT_FIELD_NAME_FILTERS = "filters";
    }

    public static final Set<String> ALLOWED_CONTENT_FIELD_NAMES;
    static {
        ALLOWED_CONTENT_FIELD_NAMES = new HashSet<>();
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_PARENT_PATH);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_INPUT);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_WEIGHT);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_INPUT_FIELDS);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_FILTERS);
    }

    public static class Builder extends FieldMapper.Builder<Builder, FilteredSuggestFieldMapper> {

        private boolean preserveSeparators = Defaults.DEFAULT_PRESERVE_SEPARATORS;
        private boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;
        private int maxInputLength = Defaults.DEFAULT_MAX_INPUT_LENGTH;
        private FilterMappings filterMappings;
        private List<String> inputFields;
        private String parentPath;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder preserveSeparators(boolean preserveSeparators) {
            this.preserveSeparators = preserveSeparators;
            return this;
        }

        public Builder preservePositionIncrements(boolean preservePositionIncrements) {
            this.preservePositionIncrements = preservePositionIncrements;
            return this;
        }

        public Builder maxInputLength(int maxInputLength) {
            if (maxInputLength <= 0) {
                throw new IllegalArgumentException(
                        Fields.MAX_INPUT_LENGTH.getPreferredName() + " must be > 0 but was [" + maxInputLength + "]");
            }
            this.maxInputLength = maxInputLength;
            return this;
        }

        public Builder filterMappings(FilterMappings filterMappings) {
            this.filterMappings = filterMappings;
            return this;
        }

        public Builder setInputFields(List<String> inputFields) {
            this.inputFields = inputFields;
            return this;
        }

        @Override
        public FilteredSuggestFieldMapper build(Mapper.BuilderContext context) {
            setupFieldType(context);
            CompletionFieldType completionFieldType = (CompletionFieldType) fieldType;
            completionFieldType.setPreservePositionIncrements(preservePositionIncrements);
            completionFieldType.setPreserveSep(preserveSeparators);
            completionFieldType.setFilterMappings(filterMappings);
            completionFieldType.setParentPath(parentPath);

            if (inputFields != null) {
                completionFieldType.setInputFields(Collections.unmodifiableList(inputFields));
            }

            return new FilteredSuggestFieldMapper(name, this.parentPath, fieldType, maxInputLength, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo);
        }

        public Builder setParentPath(String parentPath) {
            this.parentPath = parentPath;
            return this;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            FilteredSuggestFieldMapper.Builder builder = new Builder(name);
            NamedAnalyzer indexAnalyzer = null;
            NamedAnalyzer searchAnalyzer = null;
            String parentPath = null;
            boolean filterDefined = false;

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    continue;
                }
                if (Fields.ANALYZER.match(fieldName)) {
                    indexAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (Fields.SEARCH_ANALYZER.match(fieldName)) {
                    searchAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (Fields.PRESERVE_SEPARATORS.match(fieldName)) {
                    builder.preserveSeparators(Boolean.parseBoolean(fieldNode.toString()));
                    iterator.remove();
                } else if (Fields.PRESERVE_POSITION_INCREMENTS.match(fieldName)) {
                    builder.preservePositionIncrements(Boolean.parseBoolean(fieldNode.toString()));
                    iterator.remove();
                } else if (Fields.MAX_INPUT_LENGTH.match(fieldName)) {
                    builder.maxInputLength(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (Fields.INPUT_FIELDS.match(fieldName)) {
                    builder.setInputFields(getInputFields(fieldNode));
                    iterator.remove();
                } else if (Fields.PARENT_PATH.match(fieldName)) {
                    parentPath = fieldNode.toString();
                    builder.setParentPath(parentPath);
                    iterator.remove();
                } else if (Fields.FILTERS.match(fieldName)) {
                    builder.filterMappings(FilterMappings.loadMappings(fieldNode, parserContext.suggestFieldFilterIdGenerator()));
                    iterator.remove();

                    filterDefined = true;
                }
            }

            if (!filterDefined) {
                throw new MapperParsingException(
                        "mapper [" + name + "] has no definition for mandatory field [" + Fields.CONTENT_FIELD_NAME_FILTERS + "]");
            }

            if (parentPath == null) {
                throw new MapperParsingException(
                        "mapper [" + name + "] has no value for mandatory field [" + Fields.CONTENT_FIELD_NAME_PARENT_PATH + "]");
            }

            if (indexAnalyzer == null) {
                if (searchAnalyzer != null) {
                    throw new MapperParsingException(
                            "analyzer on filtered suggest field [" + name + "] must be set when search_analyzer is set");
                }
                indexAnalyzer = searchAnalyzer = parserContext.getIndexAnalyzers().get("simple");
            } else if (searchAnalyzer == null) {
                searchAnalyzer = indexAnalyzer;
            }
            builder.indexAnalyzer(indexAnalyzer);
            builder.searchAnalyzer(searchAnalyzer);

            return builder;
        }

        private List<String> getInputFields(Object fieldNode) {
            if (fieldNode instanceof List) {
                return (List<String>) fieldNode;
            }

            return Arrays.asList(fieldNode.toString());
        }

        private NamedAnalyzer getNamedAnalyzer(ParserContext parserContext, String name) {
            NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(name);
            if (analyzer == null) {
                throw new IllegalArgumentException("Can't find default or mapped analyzer with name [" + name + "]");
            }
            return analyzer;
        }
    }

    public static final class CompletionFieldType extends TermBasedFieldType {
        private static PostingsFormat postingsFormat;
        private boolean preserveSep = Defaults.DEFAULT_PRESERVE_SEPARATORS;
        private boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;

        private FilterMappings filterMappings;
        private List<String> inputFields;
        private String parentPath;

        public CompletionFieldType() {
        }

        protected CompletionFieldType(CompletionFieldType ref) {
            super(ref);
            this.preserveSep = ref.preserveSep;
            this.preservePositionIncrements = ref.preservePositionIncrements;
            this.filterMappings = ref.filterMappings;
        }

        public void setPreserveSep(boolean preserveSep) {
            checkIfFrozen();
            this.preserveSep = preserveSep;
        }

        public boolean preserveSep() {
            return preserveSep;
        }

        public void setPreservePositionIncrements(boolean preservePositionIncrements) {
            checkIfFrozen();
            this.preservePositionIncrements = preservePositionIncrements;
        }

        public boolean preservePositionIncrements() {
            return preservePositionIncrements;
        }

        public void setFilterMappings(FilterMappings filterMappings) {
            checkIfFrozen();
            this.filterMappings = filterMappings;
        }

        /**
         * Get the filter mapping associated with this filtered suggest completion field
         */
        public FilterMappings getFilterMappings() {
            return filterMappings;
        }

        /**
         * @return true if a filter mapping has been defined
         */
        public boolean hasFilterMappings() {
            return filterMappings != null;
        }

        public void setInputFields(List<String> inputFields) {
            this.inputFields = inputFields;
        }

        /**
         * Get the input field associated with this filtered suggest completion field
         */
        public List<String> getInputFields() {
            return inputFields;
        }

        /**
         * @return true if a input field has been defined
         */
        public boolean hasInputFields() {
            return inputFields != null && inputFields.isEmpty() == false;
        }

        public void setParentPath(String parentPath) {
            this.parentPath = parentPath;
        }

        public String getParentName() {
            return parentPath;
        }

        /**
         * @return postings format to use for this field-type
         */
        public static synchronized PostingsFormat postingsFormat() {
            if (postingsFormat == null) {
                postingsFormat = new Completion50PostingsFormat();
            }
            return postingsFormat;
        }

        /**
         * Completion prefix query
         */
        public CompletionQuery prefixQuery(Object value) {
            return new PrefixCompletionQuery(searchAnalyzer().analyzer(), new Term(name(), indexedValueForSearch(value)));
        }

        /**
         * Completion prefix regular expression query
         */
        public CompletionQuery regexpQuery(Object value, int flags, int maxDeterminizedStates) {
            return new RegexCompletionQuery(new Term(name(), indexedValueForSearch(value)), flags, maxDeterminizedStates);
        }

        /**
         * Completion prefix fuzzy query
         */
        public CompletionQuery fuzzyQuery(String value, Fuzziness fuzziness, int nonFuzzyPrefixLength, int minFuzzyPrefixLength,
                int maxExpansions, boolean transpositions, boolean unicodeAware) {
            return new FuzzyCompletionQuery(searchAnalyzer().analyzer(), new Term(name(), indexedValueForSearch(value)), null,
                    fuzziness.asDistance(), transpositions, nonFuzzyPrefixLength, minFuzzyPrefixLength, unicodeAware, maxExpansions);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            CompletionFieldType other = (CompletionFieldType) fieldType;

            if (preservePositionIncrements != other.preservePositionIncrements) {
                conflicts.add("mapper [" + name() + "] has different [preserve_position_increments] values");
            }
            if (preserveSep != other.preserveSep) {
                conflicts.add("mapper [" + name() + "] has different [preserve_separators] values");
            }

            if (hasFilterMappings() != other.hasFilterMappings()) {
                conflicts.add("mapper [" + name() + "] has different [filter_mappings] values");
            } else if (hasFilterMappings() && !(filterMappings.equals(other.filterMappings))) {
                conflicts.add("mapper [" + name() + "] has different [filter_mappings] values");
            }

            if (hasInputFields() != other.hasInputFields()) {
                conflicts.add("mapper [" + name() + "] has different [input_fields] values");
            } else if (hasInputFields() && !(inputFields.equals(other.inputFields))) {
                conflicts.add("mapper [" + name() + "] has different [input_fields] values");
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((filterMappings == null) ? 0 : filterMappings.hashCode());
            result = prime * result + ((inputFields == null) ? 0 : inputFields.hashCode());
            result = prime * result + ((parentPath == null) ? 0 : parentPath.hashCode());
            result = prime * result + (preservePositionIncrements ? 1231 : 1237);
            result = prime * result + (preserveSep ? 1231 : 1237);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!super.equals(obj))
                return false;
            if (getClass() != obj.getClass())
                return false;
            CompletionFieldType other = (CompletionFieldType) obj;
            if (filterMappings == null) {
                if (other.filterMappings != null)
                    return false;
            } else if (!filterMappings.equals(other.filterMappings))
                return false;
            if (inputFields == null) {
                if (other.inputFields != null)
                    return false;
            } else if (!inputFields.equals(other.inputFields))
                return false;
            if (parentPath == null) {
                if (other.parentPath != null)
                    return false;
            } else if (!parentPath.equals(other.parentPath))
                return false;
            if (preservePositionIncrements != other.preservePositionIncrements)
                return false;
            if (preserveSep != other.preserveSep)
                return false;
            return true;
        }

        @Override
        public CompletionFieldType clone() {
            return new CompletionFieldType(this);
        }
    }

    private int maxInputLength;
    private String parentPath;

    public FilteredSuggestFieldMapper(String simpleName, String parentPath, MappedFieldType fieldType, int maxInputLength,
            Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, multiFields, copyTo);
        this.parentPath = parentPath;
        this.maxInputLength = maxInputLength;
    }

    @Override
    public CompletionFieldType fieldType() {
        return (CompletionFieldType) super.fieldType();
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();

        int weight = 1;
        List<String> inputs = new ArrayList<>(4);
        boolean readFromField = true;

        Map<String, FilteredSuggestFilterValues> filterConfigs = null;

        if (token == XContentParser.Token.VALUE_STRING) {
            inputs.add(parser.text());
            multiFields.parse(this, context);
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == null) {
                    break;
                }

                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    if (!ALLOWED_CONTENT_FIELD_NAMES.contains(currentFieldName)) {
                        throw new IllegalArgumentException(
                                "Unknown field name[" + currentFieldName + "], must be one of " + ALLOWED_CONTENT_FIELD_NAMES);
                    }
                } else if (Fields.CONTENT_FIELD_NAME_FILTERS.equals(currentFieldName)) {
                    filterConfigs = new LinkedHashMap<>();

                    if (token == XContentParser.Token.START_OBJECT) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            String name = parser.currentName();
                            FilteredSuggestFilterMapping mapping = fieldType().getFilterMappings().get(name);
                            if (mapping == null) {
                                throw new ElasticsearchParseException("filter [" + name + "] is not defined");
                            } else {
                                token = parser.nextToken();
                                filterConfigs.put(name, mapping.parseContext(context, parser));
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("filter must be an object");
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (Fields.CONTENT_FIELD_NAME_INPUT.equals(currentFieldName)) {
                        inputs.add(parser.text());
                        readFromField = false;
                    }
                    if (Fields.CONTENT_FIELD_NAME_WEIGHT.equals(currentFieldName)) {
                        Number weightValue;
                        try {
                            weightValue = Long.parseLong(parser.text());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException(
                                    "Weight must be a string representing a numeric value, but was [" + parser.text() + "]");
                        }

                        if (weightValue.longValue() < 0 || weightValue.longValue() > Integer.MAX_VALUE) { // always
                                                                                                          // parse a
                                                                                                          // long to
                                                                                                          // make sure
                                                                                                          // we don't
                                                                                                          // get
                                                                                                          // overflow
                            throw new IllegalArgumentException(
                                    "weight must be in the interval [0..2147483647], but was [" + weightValue.longValue() + "]");
                        }
                        weight = weightValue.intValue();
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (Fields.CONTENT_FIELD_NAME_WEIGHT.equals(currentFieldName)) {
                        NumberType numberType = parser.numberType();
                        if (NumberType.LONG != numberType && NumberType.INT != numberType) {
                            throw new IllegalArgumentException("Weight must be an integer, but was [" + parser.numberValue() + "]");
                        }

                        Number weightValue = parser.numberValue();
                        if (weightValue.longValue() < 0 || weightValue.longValue() > Integer.MAX_VALUE) { // always
                                                                                                          // parse a
                                                                                                          // long to
                                                                                                          // make sure
                                                                                                          // we don't
                                                                                                          // get
                                                                                                          // overflow
                            throw new IllegalArgumentException(
                                    "weight must be in the interval [0..2147483647], but was [" + weightValue.longValue() + "]");
                        }
                        weight = weightValue.intValue();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (Fields.CONTENT_FIELD_NAME_INPUT.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            inputs.add(parser.text());
                        }
                        readFromField = false;
                    }
                }
            }
        }

        addFilterField(filterConfigs, inputs, context.doc(), readFromField, weight);

        return null;
    }

    private void addFilterField(Map<String, FilteredSuggestFilterValues> filterConfigs, List<String> inputs, Document doc,
            boolean readFromField, int weight) throws IOException {
        if (readFromField) {
            if (fieldType().getInputFields() == null) {
                throw new IllegalArgumentException("Either input_field at mapping or input values must be specified");
            }

            for (String fieldName : fieldType().getInputFields()) {
                IndexableField[] fields = doc.getFields(fieldName);
                if (fields.length == 0) {
                    throw new IllegalArgumentException("Input field [" + fieldName
                            + "] must be at the same document hierarchy level where suggestion field is defined");
                }

                if (fields.length == 1 && fields[0].fieldType().docValuesType() != DocValuesType.NONE) {
                    throw new IllegalArgumentException("Suggestion input field [" + fieldName + "] must be an indexed field.");
                }

                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].fieldType().docValuesType() == DocValuesType.NONE) {
                        inputs.add(fields[i].stringValue());
                    }
                }
            }
        }

        for (String input : inputs) {
            if (input.length() == 0) {
                continue;
            }

            fieldType().getFilterMappings().addField(doc, fieldType().name(), correctSubStringLen(input, maxInputLength), weight,
                    filterConfigs);
        }
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        for (int i = 0; i < context.docs().size(); i++) {
            final Document doc = context.docs().get(i);

            if (!doc.getPath().equals(parentPath)) {
                continue;
            }

            boolean docHasField = false;
            for (IndexableField field : doc.getFields()) {
                if (FilterMappings.isFilteredSuggestField(field)) {
                    docHasField = true;
                    break;
                }
            }

            if (docHasField) {
                continue;
            }

            addFilterField(null, new ArrayList<>(4), doc, true, 1);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        // no-op
    }

    public static String correctSubStringLen(String input, int len) {
        int length = Math.min(input.length(), len);

        if (Character.isHighSurrogate(input.charAt(length - 1))) {
            assert input.length() >= length + 1 && Character.isLowSurrogate(input.charAt(length));
            length = length + 1;
        }

        input = input.substring(0, length);

        return input;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName()).field(Fields.TYPE.getPreferredName(), CONTENT_TYPE);

        builder.field(Fields.ANALYZER.getPreferredName(), fieldType().indexAnalyzer().name());
        if (fieldType().indexAnalyzer().name().equals(fieldType().searchAnalyzer().name()) == false) {
            builder.field(Fields.SEARCH_ANALYZER.getPreferredName(), fieldType().searchAnalyzer().name());
        }
        builder.field(Fields.PRESERVE_SEPARATORS.getPreferredName(), fieldType().preserveSep());
        builder.field(Fields.PRESERVE_POSITION_INCREMENTS.getPreferredName(), fieldType().preservePositionIncrements());
        builder.field(Fields.MAX_INPUT_LENGTH.getPreferredName(), this.maxInputLength);

        if (fieldType().hasInputFields()) {
            builder.field(Fields.INPUT_FIELDS.getPreferredName(), fieldType().getInputFields());
        }

        builder.field(Fields.PARENT_PATH.getPreferredName(), fieldType().getParentName());

        multiFields.toXContent(builder, params);

        if (fieldType().hasFilterMappings()) {
            builder.startObject(Fields.FILTERS.getPreferredName());
            fieldType().getFilterMappings().toXContent(builder, params);
            builder.endObject();
        }

        return builder.endObject();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        FilteredSuggestFieldMapper fieldMergeWith = (FilteredSuggestFieldMapper) mergeWith;
        this.maxInputLength = fieldMergeWith.maxInputLength;
    }

}
