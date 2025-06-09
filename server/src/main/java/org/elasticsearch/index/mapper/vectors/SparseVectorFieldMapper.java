/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.inference.WeightedTokensUtils;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField} as a sparse
 * vector of features.
 */
public class SparseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "sparse_vector";
    public static final String SPARSE_VECTOR_INDEX_OPTIONS = "index_options";

    static final String ERROR_MESSAGE_7X = "[sparse_vector] field type in old 7.x indices is allowed to "
        + "contain [sparse_vector] fields, but they cannot be indexed or searched.";
    static final String ERROR_MESSAGE_8X = "The [sparse_vector] field type is not supported on indices created on versions 8.0 to 8.10.";
    static final IndexVersion PREVIOUS_SPARSE_VECTOR_INDEX_VERSION = IndexVersions.V_8_0_0;

    static final IndexVersion NEW_SPARSE_VECTOR_INDEX_VERSION = IndexVersions.NEW_SPARSE_VECTOR;
    static final IndexVersion SPARSE_VECTOR_IN_FIELD_NAMES_INDEX_VERSION = IndexVersions.SPARSE_VECTOR_IN_FIELD_NAMES_SUPPORT;
    static final IndexVersion SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION = IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT;
    static final IndexVersion SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION_8_X =
        IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT_BACKPORT_8_X;

    public static final NodeFeature SPARSE_VECTOR_INDEX_OPTIONS_FEATURE = new NodeFeature("sparse_vector.index_options_supported");

    private static SparseVectorFieldMapper toType(FieldMapper in) {
        return (SparseVectorFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).fieldType().isStored(), false);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<IndexOptions> indexOptions = new Parameter<>(
            SPARSE_VECTOR_INDEX_OPTIONS,
            true,
            () -> null,
            (n, c, o) -> parseIndexOptions(c, o),
            m -> toType(m).fieldType().indexOptions,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull();

        public Builder(String name) {
            super(name);
        }

        public Builder setStored(boolean value) {
            stored.setValue(value);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { stored, meta, indexOptions };
        }

        @Override
        public SparseVectorFieldMapper build(MapperBuilderContext context) {
            return new SparseVectorFieldMapper(
                leafName(),
                new SparseVectorFieldType(context.buildFullName(leafName()), stored.getValue(), meta.getValue(), indexOptions.getValue()),
                builderParams(this, context)
            );
        }
    }

    public IndexOptions getIndexOptions() {
        return fieldType().getIndexOptions();
    }

    private static final ConstructingObjectParser<IndexOptions, Void> INDEX_OPTIONS_PARSER = new ConstructingObjectParser<>(
        SPARSE_VECTOR_INDEX_OPTIONS,
        args -> new IndexOptions((Boolean) args[0], (TokenPruningConfig) args[1])
    );

    static {
        INDEX_OPTIONS_PARSER.declareBoolean(optionalConstructorArg(), IndexOptions.PRUNE_FIELD_NAME);
        INDEX_OPTIONS_PARSER.declareObject(optionalConstructorArg(), TokenPruningConfig.PARSER, IndexOptions.PRUNING_CONFIG_FIELD_NAME);
    }

    private static SparseVectorFieldMapper.IndexOptions parseIndexOptions(MappingParserContext context, Object propNode) {
        if (propNode == null) {
            return null;
        }

        Map<String, Object> indexOptionsMap = XContentMapValues.nodeMapValue(propNode, SPARSE_VECTOR_INDEX_OPTIONS);

        XContentParser parser = new MapXContentParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            indexOptionsMap,
            XContentType.JSON
        );

        try {
            return INDEX_OPTIONS_PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        if (c.indexVersionCreated().before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
            deprecationLogger.warn(DeprecationCategory.MAPPINGS, "sparse_vector", ERROR_MESSAGE_7X);
        } else if (c.indexVersionCreated().before(NEW_SPARSE_VECTOR_INDEX_VERSION)) {
            throw new IllegalArgumentException(ERROR_MESSAGE_8X);
        }

        return new Builder(n);
    }, notInMultiFields(CONTENT_TYPE));

    public static final class SparseVectorFieldType extends MappedFieldType {
        private final IndexOptions indexOptions;

        public SparseVectorFieldType(String name, boolean isStored, Map<String, String> meta) {
            this(name, isStored, meta, null);
        }

        public SparseVectorFieldType(
            String name,
            boolean isStored,
            Map<String, String> meta,
            @Nullable SparseVectorFieldMapper.IndexOptions indexOptions
        ) {
            super(name, true, isStored, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.indexOptions = indexOptions;
        }

        public IndexOptions getIndexOptions() {
            return indexOptions;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isVectorEmbedding() {
            return true;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[sparse_vector] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (isStored()) {
                return new SparseVectorValueFetcher(name());
            }
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return FeatureField.newLinearQuery(name(), indexedValueForSearch(value), DEFAULT_BOOST);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            if (context.getIndexSettings().getIndexVersionCreated().before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
                deprecationLogger.warn(DeprecationCategory.MAPPINGS, "sparse_vector", ERROR_MESSAGE_7X);
                return new MatchNoDocsQuery();
            } else if (context.getIndexSettings().getIndexVersionCreated().before(SPARSE_VECTOR_IN_FIELD_NAMES_INDEX_VERSION)) {
                // No support for exists queries prior to this version on 8.x
                throw new IllegalArgumentException("[sparse_vector] fields do not support [exists] queries");
            }
            return super.existsQuery(context);
        }

        public Query finalizeSparseVectorQuery(
            SearchExecutionContext context,
            String fieldName,
            List<WeightedToken> queryVectors,
            Boolean shouldPruneTokensFromQuery,
            TokenPruningConfig tokenPruningConfigFromQuery
        ) throws IOException {
            TokenPruningConfig pruningConfig = null;

            if (shouldPruneTokensFromQuery != null) {
                // if this is not null, the query is overriding the index config
                pruningConfig = shouldPruneTokensFromQuery ? tokenPruningConfigFromQuery : null;
            } else {
                // check and see if we explicitly do not prune in the index_options
                boolean explicitlyDoNotPrune = this.indexOptions != null
                    && this.indexOptions.prune != null
                    && this.indexOptions.prune == false;

                if (explicitlyDoNotPrune == false) {
                    // get the explicit pruning config from the index_options if available
                    pruningConfig = this.indexOptions != null ? this.indexOptions.pruningConfig : null;

                    // if we're still null, set the default based on the index version
                    // newer index versions default to true, while older is false
                    pruningConfig = pruningConfig == null ? getDefaultPruningConfig(context) : pruningConfig;
                }
            }

            return (pruningConfig != null)
                ? WeightedTokensUtils.queryBuilderWithPrunedTokens(fieldName, pruningConfig, queryVectors, this, context)
                : WeightedTokensUtils.queryBuilderWithAllTokens(fieldName, queryVectors, this, context);
        }

        private TokenPruningConfig getDefaultPruningConfig(SearchExecutionContext context) {
            IndexVersion indexVersion = context.indexVersionCreated();

            if (indexVersion.onOrAfter(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION)) {
                // default pruning for 9.1.0+ is true for this index
                return new TokenPruningConfig();
            }

            if (indexVersion.between(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION_8_X, IndexVersions.UPGRADE_TO_LUCENE_10_0_0)) {
                // default pruning for 8.19.0+ is true for this index
                return new TokenPruningConfig();
            }

            // the index version is before we added index_options support
            // so pruning is off by default
            return null;
        }

        private static String indexedValueForSearch(Object value) {
            if (value instanceof BytesRef) {
                return ((BytesRef) value).utf8ToString();
            }
            return value.toString();
        }
    }

    private SparseVectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams builderParams) {
        super(simpleName, mappedFieldType, builderParams);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (fieldType().isStored()) {
            return new SyntheticSourceSupport.Native(() -> new SparseVectorSyntheticFieldLoader(fullPath(), leafName()));
        }
        return super.syntheticSourceSupport();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName()).init(this);
    }

    @Override
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {

        // No support for indexing / searching 7.x sparse_vector field types
        if (context.indexSettings().getIndexVersionCreated().before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
        } else if (context.indexSettings().getIndexVersionCreated().before(NEW_SPARSE_VECTOR_INDEX_VERSION)) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_8X);
        }

        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[sparse_vector] fields must be json objects, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        final boolean isWithinLeaf = context.path().isWithinLeafObject();
        String feature = null;
        try {
            // make sure that we don't expand dots in field names while parsing
            context.path().setWithinLeafObject(true);
            for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
                if (token == Token.FIELD_NAME) {
                    feature = context.parser().currentName();
                } else if (token == Token.VALUE_NULL) {
                    // ignore feature, this is consistent with numeric fields
                } else if (token == Token.VALUE_NUMBER || token == Token.VALUE_STRING) {
                    // Use a delimiter that won't collide with subfields & escape the dots in the feature name
                    final String key = fullPath() + "\\." + feature.replace(".", "\\.");
                    float value = context.parser().floatValue(true);

                    // if we have an existing feature of the same name we'll select for the one with the max value
                    // based on recommendations from this paper: https://arxiv.org/pdf/2305.18494.pdf
                    IndexableField currentField = context.doc().getByKey(key);
                    if (currentField == null) {
                        context.doc().addWithKey(key, new XFeatureField(fullPath(), feature, value, fieldType().isStored()));
                    } else if (currentField instanceof XFeatureField && ((XFeatureField) currentField).getFeatureValue() < value) {
                        ((XFeatureField) currentField).setFeatureValue(value);
                    }
                } else {
                    throw new IllegalArgumentException(
                        "[sparse_vector] fields take hashes that map a feature to a strictly positive "
                            + "float, but got unexpected token "
                            + token
                    );
                }
            }
            if (context.indexSettings().getIndexVersionCreated().onOrAfter(SPARSE_VECTOR_IN_FIELD_NAMES_INDEX_VERSION)) {
                context.addToFieldNames(fieldType().name());
            }
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private static class SparseVectorValueFetcher implements ValueFetcher {
        private final String fieldName;
        private TermVectors termVectors;

        private SparseVectorValueFetcher(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public void setNextReader(LeafReaderContext context) {
            try {
                termVectors = context.reader().termVectors();
            } catch (IOException exc) {
                throw new UncheckedIOException(exc);
            }
        }

        @Override
        public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
            if (termVectors == null) {
                return List.of();
            }
            var terms = termVectors.get(doc, fieldName);
            if (terms == null) {
                return List.of();
            }

            var termsEnum = terms.iterator();
            PostingsEnum postingsScratch = null;
            Map<String, Float> result = new LinkedHashMap<>();
            while (termsEnum.next() != null) {
                postingsScratch = termsEnum.postings(postingsScratch);
                postingsScratch.nextDoc();
                result.put(termsEnum.term().utf8ToString(), XFeatureField.decodeFeatureValue(postingsScratch.freq()));
                assert postingsScratch.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
            }
            return List.of(result);
        }

        @Override
        public StoredFieldsSpec storedFieldsSpec() {
            return StoredFieldsSpec.NO_REQUIREMENTS;
        }
    }

    private static class SparseVectorSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private final String fullPath;
        private final String leafName;

        private TermsEnum termsDocEnum;

        private SparseVectorSyntheticFieldLoader(String fullPath, String leafName) {
            this.fullPath = fullPath;
            this.leafName = leafName;
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of();
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            var fieldInfos = leafReader.getFieldInfos().fieldInfo(fullPath);
            if (fieldInfos == null || fieldInfos.hasTermVectors() == false) {
                return null;
            }
            return docId -> {
                var terms = leafReader.termVectors().get(docId, fullPath);
                if (terms == null) {
                    return false;
                }
                termsDocEnum = terms.iterator();
                if (termsDocEnum.next() == null) {
                    termsDocEnum = null;
                    return false;
                }
                return true;
            };
        }

        @Override
        public boolean hasValue() {
            return termsDocEnum != null;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            assert termsDocEnum != null;
            PostingsEnum reuse = null;
            b.startObject(leafName);
            do {
                reuse = termsDocEnum.postings(reuse);
                reuse.nextDoc();
                b.field(termsDocEnum.term().utf8ToString(), XFeatureField.decodeFeatureValue(reuse.freq()));
            } while (termsDocEnum.next() != null);
            b.endObject();
        }

        @Override
        public String fieldName() {
            return leafName;
        }

        @Override
        public void reset() {
            termsDocEnum = null;
        }
    }

    public static class IndexOptions implements ToXContent {
        public static final ParseField PRUNE_FIELD_NAME = new ParseField("prune");
        public static final ParseField PRUNING_CONFIG_FIELD_NAME = new ParseField("pruning_config");

        final Boolean prune;
        final TokenPruningConfig pruningConfig;

        IndexOptions(@Nullable Boolean prune, @Nullable TokenPruningConfig pruningConfig) {
            if (pruningConfig != null && (prune == null || prune == false)) {
                throw new IllegalArgumentException(
                    "["
                        + SPARSE_VECTOR_INDEX_OPTIONS
                        + "] field ["
                        + PRUNING_CONFIG_FIELD_NAME.getPreferredName()
                        + "] should only be set if ["
                        + PRUNE_FIELD_NAME.getPreferredName()
                        + "] is set to true"
                );
            }

            this.prune = prune;
            this.pruningConfig = pruningConfig;
        }

        public Boolean getPrune() {
            return prune;
        }

        public TokenPruningConfig getPruningConfig() {
            return pruningConfig;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (prune != null) {
                builder.field(PRUNE_FIELD_NAME.getPreferredName(), prune);
            }
            if (pruningConfig != null) {
                builder.field(PRUNING_CONFIG_FIELD_NAME.getPreferredName(), pruningConfig);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public final boolean equals(Object other) {
            if (other == this) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            IndexOptions otherAsIndexOptions = (IndexOptions) other;
            return Objects.equals(prune, otherAsIndexOptions.prune) && Objects.equals(pruningConfig, otherAsIndexOptions.pruningConfig);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(prune, pruningConfig);
        }
    }

}
