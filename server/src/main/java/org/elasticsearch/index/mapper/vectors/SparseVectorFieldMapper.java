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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
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
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
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

import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
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
        private final IndexVersion indexVersionCreated;
        private final Parameter<Boolean> stored;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<SparseVectorIndexOptions> indexOptions = new Parameter<>(
            SPARSE_VECTOR_INDEX_OPTIONS,
            true,
            () -> null,
            (n, c, o) -> parseIndexOptions(c, o),
            m -> toType(m).fieldType().indexOptions,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull().setSerializerCheck(this::indexOptionsSerializerCheck);

        private final boolean isExcludeSourceVectors;

        public Builder(String name, IndexVersion indexVersionCreated, boolean isExcludeSourceVectors) {
            super(name);
            this.stored = Parameter.boolParam("store", false, m -> toType(m).fieldType().isStored(), () -> isExcludeSourceVectors);
            this.indexVersionCreated = indexVersionCreated;
            this.isExcludeSourceVectors = isExcludeSourceVectors;
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
            SparseVectorIndexOptions builderIndexOptions = indexOptions.getValue();
            if (builderIndexOptions == null) {
                builderIndexOptions = SparseVectorIndexOptions.getDefaultIndexOptions(indexVersionCreated);
            }

            final boolean isExcludeSourceVectorsFinal = isExcludeSourceVectors && context.isSourceSynthetic() == false && stored.get();
            return new SparseVectorFieldMapper(
                leafName(),
                new SparseVectorFieldType(
                    indexVersionCreated,
                    context.buildFullName(leafName()),
                    stored.get(),
                    meta.getValue(),
                    builderIndexOptions
                ),
                builderParams(this, context),
                isExcludeSourceVectorsFinal
            );
        }

        private boolean indexOptionsSerializerCheck(boolean includeDefaults, boolean isConfigured, SparseVectorIndexOptions value) {
            return includeDefaults || (SparseVectorIndexOptions.isDefaultOptions(value, indexVersionCreated) == false);
        }

        public void setIndexOptions(SparseVectorIndexOptions sparseVectorIndexOptions) {
            indexOptions.setValue(sparseVectorIndexOptions);
        }
    }

    public SparseVectorIndexOptions getIndexOptions() {
        return fieldType().getIndexOptions();
    }

    private static final ConstructingObjectParser<SparseVectorIndexOptions, Void> INDEX_OPTIONS_PARSER = new ConstructingObjectParser<>(
        SPARSE_VECTOR_INDEX_OPTIONS,
        args -> new SparseVectorIndexOptions((Boolean) args[0], (TokenPruningConfig) args[1])
    );

    static {
        INDEX_OPTIONS_PARSER.declareBoolean(optionalConstructorArg(), SparseVectorIndexOptions.PRUNE_FIELD_NAME);
        INDEX_OPTIONS_PARSER.declareObject(
            optionalConstructorArg(),
            TokenPruningConfig.PARSER,
            SparseVectorIndexOptions.PRUNING_CONFIG_FIELD_NAME
        );
    }

    private static SparseVectorIndexOptions parseIndexOptions(MappingParserContext context, Object propNode) {
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

        return new Builder(
            n,
            c.indexVersionCreated(),
            INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.get(c.getIndexSettings().getSettings())
        );
    }, notInMultiFields(CONTENT_TYPE));

    public static final class SparseVectorFieldType extends MappedFieldType {
        private final IndexVersion indexVersionCreated;
        private final SparseVectorIndexOptions indexOptions;

        public SparseVectorFieldType(IndexVersion indexVersionCreated, String name, boolean isStored, Map<String, String> meta) {
            this(indexVersionCreated, name, isStored, meta, null);
        }

        public SparseVectorFieldType(
            IndexVersion indexVersionCreated,
            String name,
            boolean isStored,
            Map<String, String> meta,
            @Nullable SparseVectorIndexOptions indexOptions
        ) {
            super(name, IndexType.vectors(), isStored, meta);
            this.indexVersionCreated = indexVersionCreated;
            this.indexOptions = indexOptions;
        }

        public SparseVectorIndexOptions getIndexOptions() {
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
        public TextSearchInfo getTextSearchInfo() {
            return TextSearchInfo.SIMPLE_MATCH_ONLY;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[sparse_vector] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
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
                return Queries.NO_DOCS_INSTANCE;
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
            Boolean shouldPruneTokens = shouldPruneTokensFromQuery;
            TokenPruningConfig tokenPruningConfig = tokenPruningConfigFromQuery;

            if (indexOptions != null) {
                if (shouldPruneTokens == null && indexOptions.prune != null) {
                    shouldPruneTokens = indexOptions.prune;
                }

                if (tokenPruningConfig == null && indexOptions.pruningConfig != null) {
                    tokenPruningConfig = indexOptions.pruningConfig;
                }
            }

            return (shouldPruneTokens != null && shouldPruneTokens)
                ? WeightedTokensUtils.queryBuilderWithPrunedTokens(
                    fieldName,
                    tokenPruningConfig == null ? new TokenPruningConfig() : tokenPruningConfig,
                    queryVectors,
                    this,
                    context
                )
                : WeightedTokensUtils.queryBuilderWithAllTokens(fieldName, queryVectors, this, context);
        }

        private static String indexedValueForSearch(Object value) {
            if (value instanceof BytesRef) {
                return ((BytesRef) value).utf8ToString();
            }
            return value.toString();
        }
    }

    private final boolean isExcludeSourceVectors;

    private SparseVectorFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        boolean isExcludeSourceVectors
    ) {
        super(simpleName, mappedFieldType, builderParams);
        assert isExcludeSourceVectors == false || fieldType().isStored();
        this.isExcludeSourceVectors = isExcludeSourceVectors;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (fieldType().isStored()) {
            return new SyntheticSourceSupport.Native(() -> new SparseVectorSyntheticFieldLoader(fullPath(), leafName()));
        }
        return super.syntheticSourceSupport();
    }

    @Override
    public SourceLoader.SyntheticVectorsLoader syntheticVectorsLoader() {
        if (isExcludeSourceVectors) {
            return new SyntheticVectorsPatchFieldLoader<>(
                // Recreate the object for each leaf so that different segments can be searched concurrently.
                () -> new SparseVectorSyntheticFieldLoader(fullPath(), leafName()),
                SparseVectorSyntheticFieldLoader::copyAsMap
            );
        }
        return null;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), this.fieldType().indexVersionCreated, this.isExcludeSourceVectors).init(this);
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
                        context.doc().addWithKey(key, new FeatureField(fullPath(), feature, value, fieldType().isStored()));
                    } else if (currentField instanceof FeatureField ff && ff.getFeatureValue() < value) {
                        ff.setFeatureValue(value);
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

    private static boolean indexVersionSupportsDefaultPruningConfig(IndexVersion indexVersion) {
        // default pruning for 9.1.0+ or 8.19.0+ is true for this index
        return (indexVersion.onOrAfter(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION)
            || indexVersion.between(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION_8_X, IndexVersions.UPGRADE_TO_LUCENE_10_0_0));
    }

    private static class SparseVectorSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private final String fullPath;
        private final String leafName;

        private TermsEnum termsDocEnum;
        private boolean hasValue;

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
            // Use an exists query on _field_names to distinguish documents with no value
            // from those containing an empty map.
            var existsQuery = new TermQuery(new Term(FieldNamesFieldMapper.NAME, fullPath));
            var searcher = new IndexSearcher(leafReader);
            searcher.setQueryCache(null);
            var scorer = searcher.createWeight(existsQuery, ScoreMode.COMPLETE_NO_SCORES, 0).scorer(searcher.getLeafContexts().getFirst());
            if (scorer == null) {
                return docId -> false;
            }

            var fieldInfos = leafReader.getFieldInfos().fieldInfo(fullPath);
            boolean hasTermVectors = fieldInfos != null && fieldInfos.hasTermVectors();
            return docId -> {
                termsDocEnum = null;

                if (scorer.iterator().docID() < docId) {
                    scorer.iterator().advance(docId);
                }
                if (scorer.iterator().docID() != docId) {
                    return hasValue = false;
                }

                if (hasTermVectors == false) {
                    return hasValue = true;
                }

                var terms = leafReader.termVectors().get(docId, fullPath);
                if (terms != null) {
                    termsDocEnum = terms.iterator();
                    if (termsDocEnum.next() == null) {
                        termsDocEnum = null;
                    }
                }
                return hasValue = true;
            };
        }

        @Override
        public boolean hasValue() {
            return hasValue;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            assert hasValue;
            b.startObject(leafName);
            if (termsDocEnum != null) {
                PostingsEnum reuse = null;
                do {
                    reuse = termsDocEnum.postings(reuse);
                    reuse.nextDoc();
                    b.field(termsDocEnum.term().utf8ToString(), XFeatureField.decodeFeatureValue(reuse.freq()));
                } while (termsDocEnum.next() != null);
            }
            b.endObject();
        }

        /**
         * Returns a deep-copied tokens map for the current document.
         *
         * @throws IOException if reading fails
         */
        private Map<String, Float> copyAsMap() throws IOException {
            assert hasValue;
            if (termsDocEnum == null) {
                return Map.of();
            }
            Map<String, Float> tokenMap = new LinkedHashMap<>();
            PostingsEnum reuse = null;
            do {
                reuse = termsDocEnum.postings(reuse);
                reuse.nextDoc();
                tokenMap.put(termsDocEnum.term().utf8ToString(), XFeatureField.decodeFeatureValue(reuse.freq()));
            } while (termsDocEnum.next() != null);
            return tokenMap;
        }

        @Override
        public String fieldName() {
            return fullPath;
        }

        @Override
        public void reset() {
            termsDocEnum = null;
        }
    }

    public static class SparseVectorIndexOptions implements IndexOptions {
        public static final ParseField PRUNE_FIELD_NAME = new ParseField("prune");
        public static final ParseField PRUNING_CONFIG_FIELD_NAME = new ParseField("pruning_config");
        public static final SparseVectorIndexOptions DEFAULT_PRUNING_INDEX_OPTIONS = new SparseVectorIndexOptions(
            true,
            new TokenPruningConfig()
        );

        final Boolean prune;
        final TokenPruningConfig pruningConfig;

        public SparseVectorIndexOptions(@Nullable Boolean prune, @Nullable TokenPruningConfig pruningConfig) {
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

        public static boolean isDefaultOptions(SparseVectorIndexOptions indexOptions, IndexVersion indexVersion) {
            SparseVectorIndexOptions defaultIndexOptions = indexVersionSupportsDefaultPruningConfig(indexVersion)
                ? DEFAULT_PRUNING_INDEX_OPTIONS
                : null;

            return Objects.equals(indexOptions, defaultIndexOptions);
        }

        public static SparseVectorIndexOptions getDefaultIndexOptions(IndexVersion indexVersion) {
            return indexVersionSupportsDefaultPruningConfig(indexVersion) ? DEFAULT_PRUNING_INDEX_OPTIONS : null;
        }

        public static SparseVectorIndexOptions parseFromMap(Map<String, Object> map) {
            if (map == null) {
                return null;
            }

            try {
                XContentParser parser = new MapXContentParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.IGNORE_DEPRECATIONS,
                    map,
                    XContentType.JSON
                );

                return INDEX_OPTIONS_PARSER.parse(parser, null);
            } catch (IOException ioEx) {
                throw new UncheckedIOException(ioEx);
            }
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

            SparseVectorIndexOptions otherAsIndexOptions = (SparseVectorIndexOptions) other;
            return Objects.equals(prune, otherAsIndexOptions.prune) && Objects.equals(pruningConfig, otherAsIndexOptions.pruningConfig);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(prune, pruningConfig);
        }
    }
}
