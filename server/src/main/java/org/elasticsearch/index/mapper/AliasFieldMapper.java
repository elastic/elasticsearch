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

import joptsimple.internal.Strings;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * A field mapper for alias fields.
 */
public class AliasFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "alias";

    public static class Defaults {

        public static final AliasFieldType FIELD_TYPE = new AliasFieldType();

        // the next two options are just dummy values and shouldnt be considered sensible.
        static final IndexOptions INDEX_OPTION = IndexOptions.NONE;
        static final boolean DOCS_VALUE_SET = false;

        static {
            FIELD_TYPE.freeze();
        }
    }

    // captures the values from a mapping...

    /**
     * Used to convert a property definition in a mapping where type=alias into a builder.
     */
    public static class TypeParser implements Mapper.TypeParser {

        public static final String PATH = "path";

        @Override
        public AliasFieldMapper.Builder parse(final String name,
                                              final Map<String, Object> node,
                                              final ParserContext parserContext) throws MapperParsingException {
            AliasFieldMapper.Builder builder = new AliasFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);

            String path = null;

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals(PATH)) {
                    if (propNode == null) {
                        throw new MapperParsingException(propertyPathNull());
                    }
                    path = XContentMapValues.nodeStringValue(propNode, name + "." + PATH).trim();
                    if (path.isEmpty()) {
                        throw new MapperParsingException(propertyPathEmptyOrBlank());
                    }
                    builder.path(path);
                    iterator.remove();
                }
            }

            if(null == path){
                throw new MapperParsingException(propertyPathMissing());
            }

            return builder;
        }
    }

    /**
     * Collects the properties of an alias property definition and eventually builds a AliasFieldMapper
     */
    public static class Builder extends FieldMapper.Builder<Builder, AliasFieldMapper> {

        private String path;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE, Defaults.INDEX_OPTION, Defaults.DOCS_VALUE_SET);
            this.builder = this;
        }

        public Builder path(final String path) {
            // TODO validate field names...HERE cant be null/empty etc.
            this.path = path;
            return this;
        }

        @Override
        public Builder tokenized(boolean tokenized) {
            return readOnly();
        }

        @Override
        public AliasFieldMapper build(BuilderContext context) {
          if(true)  setupFieldType(context);
            return new AliasFieldMapper(this.name, this.path, (AliasFieldType)this.fieldType);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            // override to avoid calls to other setters.
            fieldType.setName(buildFullName(context));
        }
    }

    /* @VisibleForTesting*/
    public static class AliasFieldType extends MappedFieldType{

        // @VisibleForTesting
        public static String nameInMessage(final String aliasName, final String targetName) {
            return nameInMessage(aliasName) + " -> " + nameInMessage(targetName);
        }

        final RootObjectMapper mapping;

        final String pathTo;

        /**
         * Might be initialised late.
         */
        MappedFieldType aliasTarget;

        AliasFieldType() {
            this(null, null, null, null);
        }

        AliasFieldType(final RootObjectMapper rootObjectMapper, final String name, final String pathTo, final MappedFieldType aliasTarget) {
            this.mapping = rootObjectMapper;
            this.setName(name);
            this.pathTo = pathTo;

            MappedFieldType aliasTarget0 = aliasTarget;
            if(null==aliasTarget && null != rootObjectMapper) {
                final FieldMapper fieldMapper = (FieldMapper) rootObjectMapper.mapperForPath(pathTo);
                aliasTarget0 = fieldMapper.fieldType();
            }

            if(null!=aliasTarget0) {
                // so the messages thrown will hold the alias name!
                aliasTarget0.setAlias(this); //setAliased(this);
            }
            this.aliasTarget = aliasTarget0;
        }

        public boolean isAlias() {
            return true;
        }

        public void setAlias(AliasFieldMapper.AliasFieldType alias) {
            throw new IllegalArgumentException(propertyPathToAnotherAlias(alias.nameForMessages()));
        }

        private boolean isAliasTargetPresent() {
            return null != this.aliasTarget;
        }

        MappedFieldType aliasTarget() {
            Objects.requireNonNull(this.aliasTarget, "alias target not set");
            return this.aliasTarget;
        }

        public String nameForIndex() {
            return this.aliasTarget().name();
        }

        public String nameForMessages() {
            return nameInMessage(this.name(), this.aliasTarget.name());
        }

        public MappedFieldType fieldTypeForIndex() {
            return this.aliasTarget;
        }

        @Override
        void init() {
            // overridden to stop super.init which setting some defaults by calling setters,
            // which all throw UOE because the alias target is null
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        // features...

        @Override
        public boolean isSearchable() {
            return this.aliasTarget().isSearchable();
        }

        @Override
        public boolean isAggregatable() {
            return this.aliasTarget().isAggregatable();
        }

        public void freeze() {
            super.freeze();

            // without this if, AliasFieldMapper.Defaults.FIELD_TYPE will throw
            if(this.isAliasTargetPresent()) {
                this.aliasTarget().freeze();
            }
        }

        //@Override
        public void checkCompatibility(MappedFieldType other, List<String> conflicts) {
            this.checkTypeName(other);

            final AliasFieldType aliasFieldType = (AliasFieldType) other;
            this.aliasTarget().checkCompatibility(aliasFieldType.aliasTarget(), conflicts);
        }

        // CAPABILITIES ETCs ALL read ops delegate, setters throw

        @Override
        public boolean stored() {
            return this.aliasTarget().stored();
        }

        @Override
        public void setStored(boolean value) {
            readOnly();
        }

        @Override
        public boolean tokenized() {
            return this.isAliasTargetPresent() ? this.aliasTarget().tokenized() : null;
        }

        @Override
        public void setTokenized(boolean value) {
            readOnly();
        }

        @Override
        public boolean storeTermVectors() {
            return this.aliasTarget().storeTermVectors();
        }

        @Override
        public void setStoreTermVectors(boolean value) {
            readOnly();
        }

        @Override
        public boolean storeTermVectorOffsets() {
            return this.aliasTarget().storeTermVectorOffsets();
        }

        @Override
        public void setStoreTermVectorOffsets(boolean value) {
            readOnly();
        }

        @Override
        public boolean storeTermVectorPositions() {
            return this.aliasTarget().storeTermVectorPositions();
        }

        @Override
        public void setStoreTermVectorPositions(boolean value) {
            readOnly();
        }

        @Override
        public boolean storeTermVectorPayloads() {
            return this.aliasTarget().storeTermVectorPayloads();
        }

        @Override
        public void setStoreTermVectorPayloads(boolean value) {
            readOnly();
        }

        @Override
        public boolean omitNorms() {
            return this.aliasTarget().omitNorms();
        }

        @Override
        public void setOmitNorms(boolean value) {
            readOnly();
        }

        @Override
        public IndexOptions indexOptions() {
            return this.aliasTarget().indexOptions();
        }

        @Override
        public void setIndexOptions(IndexOptions value) {
            this.aliasTarget().setIndexOptions(value);
        }

        @Override
        public void setDimensions(int dimensionCount, int dimensionNumBytes) {
            super.setDimensions(dimensionCount, dimensionNumBytes);
        }

        @Override
        public int pointDimensionCount() {
            return this.aliasTarget().pointDimensionCount();
        }

        @Override
        public int pointNumBytes() {
            return this.aliasTarget().pointNumBytes();
        }

        @Override
        public DocValuesType docValuesType() {
            return this.aliasTarget().docValuesType();
        }

        @Override
        public void setDocValuesType(DocValuesType type) {
            readOnly();
        }

        @Override
        public MappedFieldType clone() {
            return new AliasFieldType(this.mapping,
                this.name(),
                this.pathTo,
                this.isAliasTargetPresent() ? this.aliasTarget().clone() : null);
        }


        // INDEX PROPS

        @Override
        public boolean hasDocValues() {
            return this.aliasTarget().hasDocValues();
        }

        @Override
        public void setHasDocValues(boolean hasDocValues) {
            this.aliasTarget().setHasDocValues(hasDocValues);
        }

        // INDEXING OPS

        @Override
        public Object nullValue() {
            return this.aliasTarget().nullValue();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return this.aliasTarget().fielddataBuilder(fullyQualifiedIndexName);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, DateTimeZone timeZone) {
            return this.aliasTarget().docValueFormat(format, timeZone);
        }

        // MISC...

        @Override
        public boolean eagerGlobalOrdinals() {
            return this.aliasTarget().eagerGlobalOrdinals();
        }

        @Override
        public void setEagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.aliasTarget().setEagerGlobalOrdinals(eagerGlobalOrdinals);
        }

        // ANALYZER

        @Override
        public NamedAnalyzer indexAnalyzer() {
            return this.isAliasTargetPresent() ? this.aliasTarget().indexAnalyzer() : null;
        }

        @Override
        public void setIndexAnalyzer(NamedAnalyzer analyzer) {
            this.aliasTarget().setIndexAnalyzer(analyzer);
        }

        @Override
        public NamedAnalyzer searchAnalyzer() {
            return this.isAliasTargetPresent() ? this.aliasTarget().searchAnalyzer() : null;
        }

        @Override
        public void setSearchAnalyzer(NamedAnalyzer analyzer) {
            this.aliasTarget().setSearchAnalyzer(analyzer);
        }

        @Override
        public NamedAnalyzer searchQuoteAnalyzer() {
            return this.isAliasTargetPresent() ? this.aliasTarget().searchQuoteAnalyzer() : null;
        }

        @Override
        public void setSearchQuoteAnalyzer(NamedAnalyzer analyzer) {
            this.aliasTarget().setSearchQuoteAnalyzer(analyzer);
        }

        @Override
        public SimilarityProvider similarity() {
            return this.aliasTarget().similarity();
        }

        @Override
        public void setSimilarity(SimilarityProvider similarity) {
            this.aliasTarget().setSimilarity(similarity);
        }

        @Override
        public String nullValueAsString() {
            return this.aliasTarget().nullValueAsString();
        }

        @Override
        public void setNullValue(Object nullValue) {
            this.aliasTarget().setNullValue(nullValue);
        }

        // all query methods delegate to aliased target...

        @Override
        public Query existsQuery(QueryShardContext context) {
            return this.aliasTarget().existsQuery(context);
        }

        @Override
        public Query fuzzyQuery(Object value,
                                Fuzziness fuzziness,
                                int prefixLength,
                                int maxExpansions,
                                boolean transpositions) {
            return this.aliasTarget().fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions);
        }

        @Override
        public Relation isFieldWithinQuery(IndexReader reader,
                                           Object from,
                                           Object to,
                                           boolean includeLower,
                                           boolean includeUpper,
                                           DateTimeZone timeZone,
                                           DateMathParser dateMathParser,
                                           QueryRewriteContext context) throws IOException {
            return this.aliasTarget().isFieldWithinQuery(reader,
                from,
                to,
                includeLower,
                includeUpper,
                timeZone,
                dateMathParser,
                context);
        }

        @Override
        public Query nullValueQuery() {
            return this.aliasTarget().nullValueQuery();
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            return this.aliasTarget().prefixQuery(value, method, context);
        }

        @Override
        public Query queryStringTermQuery(Term term) {
            return this.aliasTarget().queryStringTermQuery(term);
        }

        @Override
        public Query rangeQuery(Object lowerTerm,
                                Object upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                ShapeRelation relation,
                                DateTimeZone timeZone,
                                DateMathParser parser,
                                QueryShardContext context) {
            return this.aliasTarget().rangeQuery(lowerTerm,
                upperTerm,
                includeLower,
                includeUpper,
                relation,
                timeZone,
                parser,
                context);
        }

        @Override
        public Query regexpQuery(String value,
                                 int flags,
                                 int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method,
                                 QueryShardContext context) {
            return this.aliasTarget().regexpQuery(value, flags, maxDeterminizedStates, method, context);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return this.aliasTarget().termQuery(value, context);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            return this.aliasTarget().termsQuery(values, context);
        }

        @Override
        public float boost() {
            return this.aliasTarget().boost();
        }

        @Override
        public void setBoost(float boost) {
            this.aliasTarget().setBoost(boost);
        }

        // DISPLAY ...
        @Override
        public Object valueForDisplay(Object value) {
            return this.aliasTarget().valueForDisplay(value);
        }

        @Override
        public boolean equals(final Object other) {
            return this == other || other instanceof AliasFieldType && this.equals0((AliasFieldType) other);
        }

        private boolean equals0(final AliasFieldType other) {
            return this.name().equals(other.name()) &&
                Objects.equals(this.pathTo, other.pathTo) &&
                Objects.equals(this.aliasTarget, other.aliasTarget); // $aliasTarget could be null
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.name(), this.pathTo, this.aliasTarget);
        }

        public String toString(){
            return CONTENT_TYPE;
        }
    }

    private final String path;

    private RootObjectMapper rootObjectMapper;

    protected AliasFieldMapper(final String simpleName, final String path, final AliasFieldType fieldType) {
        super(simpleName);

        if(Strings.isNullOrEmpty(path)){
            throw new IllegalArgumentException(propertyPathMissing());
        }
        this.path = path;
        this.fieldType = fieldType;
        this.multiFields = MultiFields.empty();
        this.copyTo = CopyTo.empty();
    }

    public MappedFieldType fieldType() {
        return this.fieldType;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Collections.<Mapper>emptyList().iterator();
    }

    /**
     * Performs some validations on the path property.
     * - does the path exist ? FAIL
     * - is the path pointing to self ? FAIL
     * - is the path pointing to another alias ? FAIL
     * @param root the root holding all mappings.
     */
    protected void rootObjectMapper(final RootObjectMapper root) {
        this.rootObjectMapper = root;

        final String path = this.path;
        final Mapper mapper = this.rootObjectMapper.mapperForPath(path);
        if(null==mapper) {
            report(propertyPathUnknown(path));
        }
        if(this == mapper) {
            report(propertyPathToSelf(path));
        }
        if(mapper instanceof AliasFieldMapper) {
            report(propertyPathToAnotherAlias(path));
        }
        if(false == mapper instanceof FieldMapper) {
            report(propertyPathNotField(path));
        }

        final FieldMapper fieldMapper = (FieldMapper) mapper;

//        final AliasFieldType type = (AliasFieldType)this.fieldType;
//        //type.aliased = fieldMapper.fieldType();
//        type.set
        fieldMapper.fieldType().setAlias((AliasFieldType)this.fieldType());
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException(indexingUnsupported(name(), this.path));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        // we only wish to write type and path fields
        builder.field("type", contentType());
        if (null!=this.path) {
            builder.field(TypeParser.PATH, this.path);
        }
    }

    // @VisibleForTesting

    static <T> T readOnly() {
        throw new UnsupportedOperationException("Fields of type [" + CONTENT_TYPE + "] are read only");
    }

    static String propertyPathMissing(){
        return "Property [" + TypeParser.PATH + "] required but is missing";
    }

    static String propertyPathNull(){
        return "Property [" + TypeParser.PATH + "] required but is null";
    }

    static String propertyPathEmptyOrBlank() {
        return "Property [" + TypeParser.PATH + "] required but is empty or blank";
    }

    static String propertyPathUnknown(final String path) {
        return "Property [" + TypeParser.PATH + "] is not mapped to a field path=[" + path + "]";
    }

    static String propertyPathToSelf(final String alias) {
        return "Property [" + TypeParser.PATH + "] points to self " + alias;
    }

    static String propertyPathToAnotherAlias(final String alias) {
        return "Property [" + TypeParser.PATH + "] points to another alias " + alias;
    }

    static String propertyPathNotField(final String path) {
        return "Property [" + TypeParser.PATH + "] must point to a field (not object) path=[" + path + "]";
    }

    static String indexingUnsupported(final String aliasPath, final String pathTo) {
        return "Field [" + aliasPath + "] is an alias field which cannot be indexed, perhaps you meant its target [" + pathTo + "]";
    }

    static void report(final String message) {
        throw new MapperException(message);
    }
}
