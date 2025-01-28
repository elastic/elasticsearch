/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.SourceFieldMetrics.NOOP;

/**
 * A Mapper for nested objects
 */
public class NestedObjectMapper extends ObjectMapper {

    public static final String CONTENT_TYPE = "nested";

    public static class Builder extends ObjectMapper.Builder {

        private Explicit<Boolean> includeInRoot = Explicit.IMPLICIT_FALSE;
        private Explicit<Boolean> includeInParent = Explicit.IMPLICIT_FALSE;
        private final IndexVersion indexCreatedVersion;
        private final Function<Query, BitSetProducer> bitSetProducer;
        private final IndexSettings indexSettings;

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            Function<Query, BitSetProducer> bitSetProducer,
            IndexSettings indexSettings
        ) {
            super(name, Optional.empty());
            this.indexCreatedVersion = indexCreatedVersion;
            this.bitSetProducer = bitSetProducer;
            this.indexSettings = indexSettings;
        }

        Builder includeInRoot(boolean includeInRoot) {
            this.includeInRoot = Explicit.explicitBoolean(includeInRoot);
            return this;
        }

        Builder includeInParent(boolean includeInParent) {
            this.includeInParent = Explicit.explicitBoolean(includeInParent);
            return this;
        }

        @Override
        public NestedObjectMapper build(MapperBuilderContext context) {
            boolean parentIncludedInRoot = this.includeInRoot.value();
            final Query parentTypeFilter;
            if (context instanceof NestedMapperBuilderContext nc) {
                // we're already inside a nested mapper, so adjust our includes
                if (nc.parentIncludedInRoot && this.includeInParent.value()) {
                    this.includeInRoot = Explicit.IMPLICIT_FALSE;
                }
                parentTypeFilter = nc.nestedTypeFilter;
            } else {
                // this is a top-level nested mapper, so include_in_parent = include_in_root
                parentIncludedInRoot |= this.includeInParent.value();
                if (this.includeInParent.value()) {
                    this.includeInRoot = Explicit.IMPLICIT_FALSE;
                }
                parentTypeFilter = Queries.newNonNestedFilter(indexCreatedVersion);
            }
            final String fullPath = context.buildFullName(leafName());
            final String nestedTypePath;
            if (indexCreatedVersion.before(IndexVersions.V_8_0_0)) {
                nestedTypePath = "__" + fullPath;
            } else {
                nestedTypePath = fullPath;
            }
            if (sourceKeepMode.orElse(SourceKeepMode.NONE) == SourceKeepMode.ARRAYS) {
                throw new MapperException(
                    "parameter [ "
                        + Mapper.SYNTHETIC_SOURCE_KEEP_PARAM
                        + " ] can't be set to ["
                        + SourceKeepMode.ARRAYS
                        + "] for nested object ["
                        + fullPath
                        + "]"
                );
            }
            final Query nestedTypeFilter = NestedPathFieldMapper.filter(indexCreatedVersion, nestedTypePath);
            NestedMapperBuilderContext nestedContext = new NestedMapperBuilderContext(
                context.buildFullName(leafName()),
                context.isSourceSynthetic(),
                context.isDataStream(),
                context.parentObjectContainsDimensions(),
                nestedTypeFilter,
                parentIncludedInRoot,
                context.getDynamic(dynamic),
                context.getMergeReason()
            );
            return new NestedObjectMapper(
                leafName(),
                fullPath,
                buildMappers(nestedContext),
                enabled,
                dynamic,
                sourceKeepMode,
                includeInParent,
                includeInRoot,
                parentTypeFilter,
                nestedTypePath,
                nestedTypeFilter,
                bitSetProducer,
                indexSettings
            );
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            if (parseSubobjects(node).isPresent()) {
                throw new MapperParsingException("Nested type [" + name + "] does not support [subobjects] parameter");
            }
            NestedObjectMapper.Builder builder = new NestedObjectMapper.Builder(
                name,
                parserContext.indexVersionCreated(),
                parserContext::bitSetProducer,
                parserContext.getIndexSettings()
            );
            parseNested(name, node, builder);
            parseObjectFields(node, parserContext, builder);
            return builder;
        }

        protected static void parseNested(String name, Map<String, Object> node, NestedObjectMapper.Builder builder) {
            Object fieldNode = node.get("include_in_parent");
            if (fieldNode != null) {
                boolean includeInParent = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_parent");
                builder.includeInParent(includeInParent);
                node.remove("include_in_parent");
            }
            fieldNode = node.get("include_in_root");
            if (fieldNode != null) {
                boolean includeInRoot = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_root");
                builder.includeInRoot(includeInRoot);
                node.remove("include_in_root");
            }
        }
    }

    static class NestedMapperBuilderContext extends MapperBuilderContext {
        final boolean parentIncludedInRoot;
        final Query nestedTypeFilter;

        NestedMapperBuilderContext(
            String path,
            boolean isSourceSynthetic,
            boolean isDataStream,
            boolean parentObjectContainsDimensions,
            Query nestedTypeFilter,
            boolean parentIncludedInRoot,
            Dynamic dynamic,
            MapperService.MergeReason mergeReason
        ) {
            super(path, isSourceSynthetic, isDataStream, parentObjectContainsDimensions, dynamic, mergeReason, true);
            this.parentIncludedInRoot = parentIncludedInRoot;
            this.nestedTypeFilter = nestedTypeFilter;
        }

        @Override
        public MapperBuilderContext createChildContext(String name, Dynamic dynamic) {
            return new NestedMapperBuilderContext(
                buildFullName(name),
                isSourceSynthetic(),
                isDataStream(),
                parentObjectContainsDimensions(),
                nestedTypeFilter,
                parentIncludedInRoot,
                getDynamic(dynamic),
                getMergeReason()
            );
        }
    }

    private final Explicit<Boolean> includeInRoot;
    private final Explicit<Boolean> includeInParent;
    // The query to identify parent documents
    private final Query parentTypeFilter;
    // The path of the nested field
    private final String nestedTypePath;
    // The query to identify nested documents at this level
    private final Query nestedTypeFilter;
    // Function to create a bitset for identifying parent documents
    private final Function<Query, BitSetProducer> bitsetProducer;
    private final IndexSettings indexSettings;

    NestedObjectMapper(
        String name,
        String fullPath,
        Map<String, Mapper> mappers,
        Explicit<Boolean> enabled,
        ObjectMapper.Dynamic dynamic,
        Optional<SourceKeepMode> sourceKeepMode,
        Explicit<Boolean> includeInParent,
        Explicit<Boolean> includeInRoot,
        Query parentTypeFilter,
        String nestedTypePath,
        Query nestedTypeFilter,
        Function<Query, BitSetProducer> bitsetProducer,
        IndexSettings indexSettings
    ) {
        super(name, fullPath, enabled, Optional.empty(), sourceKeepMode, dynamic, mappers);
        this.parentTypeFilter = parentTypeFilter;
        this.nestedTypePath = nestedTypePath;
        this.nestedTypeFilter = nestedTypeFilter;
        this.includeInParent = includeInParent;
        this.includeInRoot = includeInRoot;
        this.bitsetProducer = bitsetProducer;
        this.indexSettings = indexSettings;
    }

    public IndexSettings indexSettings() {
        return indexSettings;
    }

    public Query parentTypeFilter() {
        return parentTypeFilter;
    }

    public Query nestedTypeFilter() {
        return this.nestedTypeFilter;
    }

    public String nestedTypePath() {
        return this.nestedTypePath;
    }

    @Override
    public boolean isNested() {
        return true;
    }

    public boolean isIncludeInParent() {
        return this.includeInParent.value();
    }

    public boolean isIncludeInRoot() {
        return this.includeInRoot.value();
    }

    public Function<Query, BitSetProducer> bitsetProducer() {
        return bitsetProducer;
    }

    public Map<String, Mapper> getChildren() {
        return this.mappers;
    }

    @Override
    public ObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        NestedObjectMapper.Builder builder = new NestedObjectMapper.Builder(leafName(), indexVersionCreated, bitsetProducer, indexSettings);
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        builder.includeInRoot = includeInRoot;
        builder.includeInParent = includeInParent;
        return builder;
    }

    @Override
    NestedObjectMapper withoutMappers() {
        return new NestedObjectMapper(
            leafName(),
            fullPath(),
            Map.of(),
            enabled,
            dynamic,
            sourceKeepMode,
            includeInParent,
            includeInRoot,
            parentTypeFilter,
            nestedTypePath,
            nestedTypeFilter,
            bitsetProducer,
            indexSettings
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(leafName());
        builder.field("type", CONTENT_TYPE);
        if (includeInParent.explicit() && includeInParent.value()) {
            builder.field("include_in_parent", includeInParent.value());
        }
        if (includeInRoot.value()) {
            builder.field("include_in_root", includeInRoot.value());
        }
        if (dynamic != null) {
            builder.field("dynamic", dynamic.name().toLowerCase(Locale.ROOT));
        }
        if (isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", enabled.value());
        }
        if (sourceKeepMode.isPresent()) {
            builder.field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, sourceKeepMode.get());
        }
        serializeMappers(builder, params);
        return builder.endObject();
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith, MapperMergeContext parentMergeContext) {
        if ((mergeWith instanceof NestedObjectMapper) == false) {
            MapperErrors.throwNestedMappingConflictError(mergeWith.fullPath());
        }
        NestedObjectMapper mergeWithObject = (NestedObjectMapper) mergeWith;

        final MapperService.MergeReason reason = parentMergeContext.getMapperBuilderContext().getMergeReason();
        var mergeResult = MergeResult.build(this, mergeWithObject, parentMergeContext);
        Explicit<Boolean> incInParent = this.includeInParent;
        Explicit<Boolean> incInRoot = this.includeInRoot;
        if (reason == MapperService.MergeReason.INDEX_TEMPLATE) {
            if (mergeWithObject.includeInParent.explicit()) {
                incInParent = mergeWithObject.includeInParent;
            }
            if (mergeWithObject.includeInRoot.explicit()) {
                incInRoot = mergeWithObject.includeInRoot;
            }
        } else {
            if (includeInParent.value() != mergeWithObject.includeInParent.value()) {
                throw new MapperException("the [include_in_parent] parameter can't be updated on a nested object mapping");
            }
            if (includeInRoot.value() != mergeWithObject.includeInRoot.value()) {
                throw new MapperException("the [include_in_root] parameter can't be updated on a nested object mapping");
            }
        }
        MapperBuilderContext parentBuilderContext = parentMergeContext.getMapperBuilderContext();
        if (parentBuilderContext instanceof NestedMapperBuilderContext nc) {
            if (nc.parentIncludedInRoot && incInParent.value()) {
                incInRoot = Explicit.IMPLICIT_FALSE;
            }
        } else {
            if (incInParent.value()) {
                incInRoot = Explicit.IMPLICIT_FALSE;
            }
        }
        return new NestedObjectMapper(
            leafName(),
            fullPath(),
            mergeResult.mappers(),
            mergeResult.enabled(),
            mergeResult.dynamic(),
            mergeResult.sourceKeepMode(),
            incInParent,
            incInRoot,
            parentTypeFilter,
            nestedTypePath,
            nestedTypeFilter,
            bitsetProducer,
            indexSettings
        );
    }

    @Override
    protected MapperMergeContext createChildContext(MapperMergeContext mapperMergeContext, String name) {
        MapperBuilderContext mapperBuilderContext = mapperMergeContext.getMapperBuilderContext();
        boolean parentIncludedInRoot = this.includeInRoot.value();
        if (mapperBuilderContext instanceof NestedMapperBuilderContext == false) {
            parentIncludedInRoot |= this.includeInParent.value();
        }
        return mapperMergeContext.createChildContext(
            new NestedMapperBuilderContext(
                mapperBuilderContext.buildFullName(name),
                mapperBuilderContext.isSourceSynthetic(),
                mapperBuilderContext.isDataStream(),
                mapperBuilderContext.parentObjectContainsDimensions(),
                nestedTypeFilter,
                parentIncludedInRoot,
                mapperBuilderContext.getDynamic(dynamic),
                mapperBuilderContext.getMergeReason()
            )
        );
    }

    @Override
    SourceLoader.SyntheticFieldLoader syntheticFieldLoader(SourceFilter filter, Collection<Mapper> mappers, boolean isFragment) {
        // IgnoredSourceFieldMapper integration takes care of writing the source for nested objects that enabled store_array_source.
        if (sourceKeepMode.orElse(SourceKeepMode.NONE) == SourceKeepMode.ALL) {
            // IgnoredSourceFieldMapper integration takes care of writing the source for the nested object.
            return SourceLoader.SyntheticFieldLoader.NOTHING;
        }

        SourceLoader sourceLoader = new SourceLoader.Synthetic(filter, () -> super.syntheticFieldLoader(filter, mappers, true), NOOP);
        // Some synthetic source use cases require using _ignored_source field
        var requiredStoredFields = IgnoredSourceFieldMapper.ensureLoaded(sourceLoader.requiredStoredFields(), indexSettings);
        // force sequential access since nested fields are indexed per block
        var storedFieldLoader = org.elasticsearch.index.fieldvisitor.StoredFieldLoader.create(false, requiredStoredFields, true);
        return new NestedSyntheticFieldLoader(
            storedFieldLoader,
            sourceLoader,
            () -> bitsetProducer.apply(parentTypeFilter),
            nestedTypeFilter
        );
    }

    private class NestedSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
        private final org.elasticsearch.index.fieldvisitor.StoredFieldLoader storedFieldLoader;
        private final SourceLoader sourceLoader;
        private final Supplier<BitSetProducer> parentBitSetProducer;
        private final Query childFilter;

        private LeafStoredFieldLoader leafStoredFieldLoader;
        private SourceLoader.Leaf leafSourceLoader;
        private final List<Integer> children = new ArrayList<>();

        private NestedSyntheticFieldLoader(
            org.elasticsearch.index.fieldvisitor.StoredFieldLoader storedFieldLoader,
            SourceLoader sourceLoader,
            Supplier<BitSetProducer> parentBitSetProducer,
            Query childFilter
        ) {
            this.storedFieldLoader = storedFieldLoader;
            this.sourceLoader = sourceLoader;
            this.parentBitSetProducer = parentBitSetProducer;
            this.childFilter = childFilter;
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            this.children.clear();
            this.leafStoredFieldLoader = storedFieldLoader.getLoader(leafReader.getContext(), null);
            this.leafSourceLoader = sourceLoader.leaf(leafReader, null);

            IndexSearcher searcher = new IndexSearcher(leafReader);
            searcher.setQueryCache(null);
            var childScorer = searcher.createWeight(childFilter, ScoreMode.COMPLETE_NO_SCORES, 1f).scorer(leafReader.getContext());
            if (childScorer != null) {
                var parentDocs = parentBitSetProducer.get().getBitSet(leafReader.getContext());
                return parentDoc -> {
                    collectChildren(parentDoc, parentDocs, childScorer.iterator());
                    return children.size() > 0;
                };
            } else {
                return parentDoc -> false;
            }
        }

        private List<Integer> collectChildren(int parentDoc, BitSet parentDocs, DocIdSetIterator childIt) throws IOException {
            assert parentDoc < 0 || parentDocs.get(parentDoc) : "wrong context, doc " + parentDoc + " is not a parent of " + nestedTypePath;
            final int prevParentDoc = parentDoc > 0 ? parentDocs.prevSetBit(parentDoc - 1) : -1;
            int childDocId = childIt.docID();
            if (childDocId <= prevParentDoc) {
                childDocId = childIt.advance(prevParentDoc + 1);
            }

            children.clear();
            for (; childDocId < parentDoc; childDocId = childIt.nextDoc()) {
                children.add(childDocId);
            }
            return children;
        }

        @Override
        public boolean hasValue() {
            return children.size() > 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            assert (children != null && children.size() > 0);
            if (children.size() == 1) {
                b.field(leafName());
                leafStoredFieldLoader.advanceTo(children.get(0));
                leafSourceLoader.write(leafStoredFieldLoader, children.get(0), b);
            } else {
                b.startArray(leafName());
                for (int childId : children) {
                    leafStoredFieldLoader.advanceTo(childId);
                    leafSourceLoader.write(leafStoredFieldLoader, childId, b);
                }
                b.endArray();
            }
        }

        @Override
        public String fieldName() {
            return NestedObjectMapper.this.fullPath();
        }

        @Override
        public void reset() {
            children.clear();
        }
    }
}
