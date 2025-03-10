/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An index abstraction is a reference to one or more concrete indices.
 * An index abstraction has a unique name and encapsulates all the {@link Index} instances it is pointing to.
 * Also depending on type it may refer to a single or many concrete indices and may or may not have a write index.
 */
public interface IndexAbstraction {

    /**
     * @return the type of the index abstraction
     */
    Type getType();

    /**
     * @return the name of the index abstraction
     */
    String getName();

    /**
     * @return All {@link Index} of all concrete indices this index abstraction is referring to.
     */
    List<Index> getIndices();

    /**
     * It retrieves the failure indices of an index abstraction given it supports the failure store.
     * @param metadata certain abstractions require the project matadata to lazily retrieve the failure indices.
     * @return All concrete failure indices this index abstraction is referring to. If the failure store is
     * not supported, it returns an empty list.
     */
    default List<Index> getFailureIndices(@Nullable ProjectMetadata metadata) {
        return List.of();
    }

    /**
     * A write index is a dedicated concrete index, that accepts all the new documents that belong to an index abstraction.
     * <p>
     * A write index may also be a regular concrete index of a index abstraction and may therefore also be returned
     * by {@link #getIndices()}. An index abstraction may also not have a dedicated write index.
     *
     * @return the write index of this index abstraction or
     * <code>null</code> if this index abstraction doesn't have a write index.
     */
    @Nullable
    Index getWriteIndex();

    /**
     * A write failure index is a dedicated concrete index, that accepts all the new documents that belong to the failure store of
     * an index abstraction. Only an index abstraction with true {@link #isDataStreamRelated()} supports a failure store.
     * @param metadata certain index abstraction require the project metadata to lazily retrieve the failure indices
     * @return the write failure index of this index abstraction or <code>null</code> if this index abstraction doesn't have
     * a write failure index or it does not support the failure store.
     */
    @Nullable
    default Index getWriteFailureIndex(ProjectMetadata metadata) {
        return null;
    }

    default Index getWriteIndex(IndexRequest request, ProjectMetadata metadata) {
        return getWriteIndex();
    }

    /**
     * @return the data stream to which this index belongs or <code>null</code> if this is not a concrete index or
     * if it is a concrete index that does not belong to a data stream.
     */
    @Nullable
    DataStream getParentDataStream();

    /**
     * @return whether this index abstraction is hidden or not
     */
    boolean isHidden();

    /**
     * @return whether this index abstraction should be treated as a system index or not
     */
    boolean isSystem();

    /**
     * @return whether this index abstraction is related to data streams
     */
    default boolean isDataStreamRelated() {
        return false;
    }

    /**
     * @return whether this index abstraction is a failure index of a data stream
     */
    default boolean isFailureIndexOfDataStream() {
        return false;
    }

    /**
     * An index abstraction type.
     */
    enum Type {

        /**
         * An index abstraction that refers to a single concrete index.
         * This concrete index is also the write index.
         */
        CONCRETE_INDEX("concrete index"),

        /**
         * An index abstraction that refers to an alias.
         * An alias typically refers to many concrete indices and
         * may have a write index.
         */
        ALIAS("alias"),

        /**
         * An index abstraction that refers to a data stream.
         * A data stream typically has multiple backing indices, the latest of which
         * is the target for index requests.
         */
        DATA_STREAM("data_stream");

        private final String displayName;

        Type(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    /**
     * Represents an concrete index and encapsulates its {@link IndexMetadata}
     */
    class ConcreteIndex implements IndexAbstraction {

        private final Index concreteIndex;
        private final boolean isHidden;
        private final boolean isSystem;
        private final DataStream dataStream;

        public ConcreteIndex(IndexMetadata indexMetadata, DataStream dataStream) {
            // note: don't capture a reference to the indexMetadata here
            this.concreteIndex = indexMetadata.getIndex();
            this.isHidden = indexMetadata.isHidden();
            this.isSystem = indexMetadata.isSystem();
            this.dataStream = dataStream;
        }

        public ConcreteIndex(IndexMetadata indexMetadata) {
            this(indexMetadata, null);
        }

        @Override
        public String getName() {
            return concreteIndex.getName();
        }

        @Override
        public Type getType() {
            return Type.CONCRETE_INDEX;
        }

        @Override
        public List<Index> getIndices() {
            return List.of(concreteIndex);
        }

        @Override
        public Index getWriteIndex() {
            return concreteIndex;
        }

        @Override
        public DataStream getParentDataStream() {
            return dataStream;
        }

        @Override
        public boolean isFailureIndexOfDataStream() {
            return getParentDataStream() != null && getParentDataStream().isFailureStoreIndex(getName());
        }

        @Override
        public boolean isHidden() {
            return isHidden;
        }

        @Override
        public boolean isSystem() {
            return isSystem;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConcreteIndex that = (ConcreteIndex) o;
            return isHidden == that.isHidden
                && isSystem == that.isSystem
                && concreteIndex.equals(that.concreteIndex)
                && Objects.equals(dataStream, that.dataStream);
        }

        @Override
        public int hashCode() {
            return Objects.hash(concreteIndex, isHidden, isSystem, dataStream);
        }
    }

    /**
     * Represents an alias and groups all {@link IndexMetadata} instances sharing the same alias name together.
     */
    class Alias implements IndexAbstraction {

        private final String aliasName;
        private final List<Index> referenceIndices;
        private final Index writeIndex;
        private final boolean isHidden;
        private final boolean isSystem;
        private final boolean dataStreamAlias;
        private final List<String> dataStreams;

        public Alias(AliasMetadata aliasMetadata, List<IndexMetadata> indexMetadatas) {
            // note: don't capture a reference to any of these indexMetadata here
            this.aliasName = aliasMetadata.getAlias();
            this.referenceIndices = new ArrayList<>(indexMetadatas.size());
            boolean isSystem = true;
            Index widx = null;
            for (IndexMetadata imd : indexMetadatas) {
                this.referenceIndices.add(imd.getIndex());
                if (Boolean.TRUE.equals(imd.getAliases().get(aliasName).writeIndex())) {
                    if (widx != null) {
                        throw new IllegalStateException("write indices size can only be 0 or 1, but is at least 2");
                    }
                    widx = imd.getIndex();
                }
                isSystem = isSystem && imd.isSystem();
            }
            this.referenceIndices.sort(Index.COMPARE_BY_NAME);

            if (widx == null && indexMetadatas.size() == 1 && indexMetadatas.get(0).getAliases().get(aliasName).writeIndex() == null) {
                widx = indexMetadatas.get(0).getIndex();
            }
            this.writeIndex = widx;

            this.isHidden = aliasMetadata.isHidden() == null ? false : aliasMetadata.isHidden();
            this.isSystem = isSystem;
            dataStreamAlias = false;
            dataStreams = List.of();
        }

        public Alias(
            DataStreamAlias dataStreamAlias,
            List<Index> indicesOfAllDataStreams,
            Index writeIndexOfWriteDataStream,
            List<String> dataStreams
        ) {
            this.aliasName = dataStreamAlias.getName();
            this.referenceIndices = indicesOfAllDataStreams;
            this.writeIndex = writeIndexOfWriteDataStream;
            this.isHidden = false;
            this.isSystem = false;
            this.dataStreamAlias = true;
            this.dataStreams = dataStreams;
        }

        @Override
        public Type getType() {
            return Type.ALIAS;
        }

        public String getName() {
            return aliasName;
        }

        @Override
        public List<Index> getIndices() {
            return referenceIndices;
        }

        @Override
        public List<Index> getFailureIndices(ProjectMetadata metadata) {
            if (isDataStreamRelated() == false) {
                return List.of();
            }
            assert metadata != null : "metadata must not be null to be able to retrieve the failure indices";
            List<Index> failureIndices = new ArrayList<>();
            for (String dataStreamName : dataStreams) {
                DataStream dataStream = metadata.dataStreams().get(dataStreamName);
                if (dataStream != null && dataStream.getFailureIndices().isEmpty() == false) {
                    failureIndices.addAll(dataStream.getFailureIndices());
                }
            }
            return failureIndices;
        }

        @Nullable
        public Index getWriteIndex() {
            return writeIndex;
        }

        @Nullable
        @Override
        public Index getWriteFailureIndex(ProjectMetadata metadata) {
            if (isDataStreamRelated() == false || writeIndex == null) {
                return null;
            }
            assert metadata != null : "metadata must not be null to be able to retrieve the failure indices";
            DataStream dataStream = metadata.getIndicesLookup().get(writeIndex.getName()).getParentDataStream();
            return dataStream == null ? null : dataStream.getWriteFailureIndex();
        }

        @Override
        public Index getWriteIndex(IndexRequest request, ProjectMetadata project) {
            if (dataStreamAlias == false) {
                return getWriteIndex();
            }

            return project.getIndicesLookup().get(getWriteIndex().getName()).getParentDataStream().getWriteIndex(request, project);
        }

        @Override
        public DataStream getParentDataStream() {
            // aliases may not be part of a data stream
            return null;
        }

        @Override
        public boolean isHidden() {
            return isHidden;
        }

        @Override
        public boolean isSystem() {
            return isSystem;
        }

        @Override
        public boolean isDataStreamRelated() {
            return dataStreamAlias;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Alias alias = (Alias) o;
            return isHidden == alias.isHidden
                && isSystem == alias.isSystem
                && dataStreamAlias == alias.dataStreamAlias
                && aliasName.equals(alias.aliasName)
                && referenceIndices.equals(alias.referenceIndices)
                && Objects.equals(writeIndex, alias.writeIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aliasName, referenceIndices, writeIndex, isHidden, isSystem, dataStreamAlias);
        }
    }
}
