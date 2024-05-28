/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

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

        public Builder(String name, IndexVersion indexCreatedVersion, Function<Query, BitSetProducer> bitSetProducer) {
            super(name, Explicit.IMPLICIT_TRUE);
            this.indexCreatedVersion = indexCreatedVersion;
            this.bitSetProducer = bitSetProducer;
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
            if (context instanceof NestedMapperBuilderContext nc) {
                // we're already inside a nested mapper, so adjust our includes
                if (nc.parentIncludedInRoot && this.includeInParent.value()) {
                    this.includeInRoot = Explicit.IMPLICIT_FALSE;
                }
            } else {
                // this is a top-level nested mapper, so include_in_parent = include_in_root
                parentIncludedInRoot |= this.includeInParent.value();
                if (this.includeInParent.value()) {
                    this.includeInRoot = Explicit.IMPLICIT_FALSE;
                }
            }
            NestedMapperBuilderContext nestedContext = new NestedMapperBuilderContext(
                context.buildFullName(name()),
                parentIncludedInRoot,
                context.getDynamic(dynamic),
                context.getMergeReason()
            );
            final String fullPath = context.buildFullName(name());
            final String nestedTypePath;
            if (indexCreatedVersion.before(IndexVersions.V_8_0_0)) {
                nestedTypePath = "__" + fullPath;
            } else {
                nestedTypePath = fullPath;
            }
            return new NestedObjectMapper(
                name(),
                fullPath,
                buildMappers(nestedContext),
                enabled,
                dynamic,
                includeInParent,
                includeInRoot,
                nestedTypePath,
                NestedPathFieldMapper.filter(indexCreatedVersion, nestedTypePath),
                bitSetProducer,
                indexCreatedVersion
            );
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            if (parseSubobjects(node).explicit()) {
                throw new MapperParsingException("Nested type [" + name + "] does not support [subobjects] parameter");
            }
            NestedObjectMapper.Builder builder = new NestedObjectMapper.Builder(
                name,
                parserContext.indexVersionCreated(),
                query -> parserContext.bitSetProducer(query)
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

    private static class NestedMapperBuilderContext extends MapperBuilderContext {

        final boolean parentIncludedInRoot;

        NestedMapperBuilderContext(String path, boolean parentIncludedInRoot, Dynamic dynamic, MapperService.MergeReason mergeReason) {
            super(path, false, false, false, dynamic, mergeReason);
            this.parentIncludedInRoot = parentIncludedInRoot;
        }

        @Override
        public MapperBuilderContext createChildContext(String name, Dynamic dynamic) {
            return new NestedMapperBuilderContext(buildFullName(name), parentIncludedInRoot, getDynamic(dynamic), getMergeReason());
        }
    }

    private final Explicit<Boolean> includeInRoot;
    private final Explicit<Boolean> includeInParent;
    private final String nestedTypePath;
    private final Query nestedTypeFilter;
    private final Function<Query, BitSetProducer> bitSetProducer;
    private final IndexVersion indexVersionCreated;

    NestedObjectMapper(
        String name,
        String fullPath,
        Map<String, Mapper> mappers,
        Explicit<Boolean> enabled,
        ObjectMapper.Dynamic dynamic,
        Explicit<Boolean> includeInParent,
        Explicit<Boolean> includeInRoot,
        String nestedTypePath,
        Query nestedTypeFilter,
        Function<Query, BitSetProducer> bitSetProducer,
        IndexVersion indexVersionCreated
    ) {
        super(name, fullPath, enabled, Explicit.IMPLICIT_TRUE, Explicit.IMPLICIT_FALSE, dynamic, mappers);
        this.nestedTypePath = nestedTypePath;
        this.nestedTypeFilter = nestedTypeFilter;
        this.includeInParent = includeInParent;
        this.includeInRoot = includeInRoot;
        this.bitSetProducer = bitSetProducer;
        this.indexVersionCreated = indexVersionCreated;
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

    public Map<String, Mapper> getChildren() {
        return this.mappers;
    }

    @Override
    public ObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        NestedObjectMapper.Builder builder = new NestedObjectMapper.Builder(simpleName(), indexVersionCreated, bitSetProducer);
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        builder.includeInRoot = includeInRoot;
        builder.includeInParent = includeInParent;
        return builder;
    }

    @Override
    NestedObjectMapper withoutMappers() {
        return new NestedObjectMapper(
            simpleName(),
            fullPath(),
            Map.of(),
            enabled,
            dynamic,
            includeInParent,
            includeInRoot,
            nestedTypePath,
            nestedTypeFilter,
            bitSetProducer,
            indexVersionCreated
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
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
        serializeMappers(builder, params);
        return builder.endObject();
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith, MapperMergeContext parentMergeContext) {
        if ((mergeWith instanceof NestedObjectMapper) == false) {
            MapperErrors.throwNestedMappingConflictError(mergeWith.name());
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
            simpleName(),
            fullPath(),
            mergeResult.mappers(),
            mergeResult.enabled(),
            mergeResult.dynamic(),
            incInParent,
            incInRoot,
            nestedTypePath,
            nestedTypeFilter,
            bitSetProducer,
            indexVersionCreated
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
                parentIncludedInRoot,
                mapperBuilderContext.getDynamic(dynamic),
                mapperBuilderContext.getMergeReason()
            )
        );
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader(NestedScope nestedScope) {
        final NestedObjectMapper mapper = this;
        SourceLoader sourceLoader = new SourceLoader.Synthetic(() -> {
            nestedScope.nextLevel(mapper);
            try {
                return super.syntheticFieldLoader(nestedScope, mappers.values().stream(), true);
            } finally {
                nestedScope.previousLevel();
            }
        }, NOOP);
        var storedFieldsLoader = StoredFieldLoader.create(false, sourceLoader.requiredStoredFields());
        var parentFilter = nestedScope.getObjectMapper() == null
            ? Queries.newNonNestedFilter(indexVersionCreated)
            : nestedScope.getObjectMapper().nestedTypeFilter();
        return new NestedFieldLoader(storedFieldsLoader, sourceLoader, () -> bitSetProducer.apply(parentFilter), nestedTypeFilter);
    }

    private class NestedFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private final org.elasticsearch.index.fieldvisitor.StoredFieldLoader storedFieldsLoader;
        private final SourceLoader sourceLoader;
        private final Supplier<BitSetProducer> parentBitSetProducer;
        private final Query childFilter;

        private List<Integer> children;
        private LeafStoredFieldLoader leafStoredFields;
        private SourceLoader.Leaf leafSource;

        private NestedFieldLoader(
            org.elasticsearch.index.fieldvisitor.StoredFieldLoader storedFieldsLoader,
            SourceLoader sourceLoader,
            Supplier<BitSetProducer> parentBitSetProducer,
            Query childFilter
        ) {
            this.storedFieldsLoader = storedFieldsLoader;
            this.sourceLoader = sourceLoader;
            this.parentBitSetProducer = parentBitSetProducer;
            this.childFilter = childFilter;
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of();
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            this.children = null;
            this.leafStoredFields = storedFieldsLoader.getLoader(leafReader.getContext(), null);
            this.leafSource = sourceLoader.leaf(leafReader, null);
            IndexSearcher searcher = new IndexSearcher(leafReader);
            searcher.setQueryCache(null);
            var childScorer = searcher.createWeight(childFilter, ScoreMode.COMPLETE_NO_SCORES, 1f).scorer(leafReader.getContext());
            var parentDocs = parentBitSetProducer.get().getBitSet(leafReader.getContext());
            return parentDoc -> {
                this.children = childScorer != null ? getChildren(parentDoc, parentDocs, childScorer.iterator()) : List.of();
                return children.size() > 0;
            };
        }

        @Override
        public boolean hasValue() {
            return children.size() > 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            assert (children != null && children.size() > 0);
            switch (children.size()) {
                case 1:
                    b.startObject(simpleName());
                    leafStoredFields.advanceTo(children.get(0));
                    leafSource.write(leafStoredFields, children.get(0), b);
                    b.endObject();
                    break;

                default:
                    b.startArray(simpleName());
                    for (int childId : children) {
                        b.startObject();
                        leafStoredFields.advanceTo(childId);
                        leafSource.write(leafStoredFields, childId, b);
                        b.endObject();
                    }
                    b.endArray();
                    break;
            }
        }

        @Override
        public String fieldName() {
            return name();
        }
    }

    private static List<Integer> getChildren(int parentDoc, BitSet parentDocs, DocIdSetIterator childIt) throws IOException {
        final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
        int childDocId = childIt.docID();
        if (childDocId <= prevParentDoc) {
            childDocId = childIt.advance(prevParentDoc + 1);
        }

        List<Integer> res = new ArrayList<>();
        for (; childDocId < parentDoc; childDocId = childIt.nextDoc()) {
            res.add(childDocId);
        }
        return res;
    }
}
