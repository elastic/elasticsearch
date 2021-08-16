/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING;

/**
 * An index abstraction is a reference to one or more concrete indices.
 * An index abstraction has a unique name and encapsulates all the  {@link IndexMetadata} instances it is pointing to.
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
     * @return All {@link IndexMetadata} of all concrete indices this index abstraction is referring to.
     */
    List<IndexMetadata> getIndices();

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
    IndexMetadata getWriteIndex();

    /**
     * @return the data stream to which this index belongs or <code>null</code> if this is not a concrete index or
     * if it is a concrete index that does not belong to a data stream.
     */
    @Nullable DataStream getParentDataStream();

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
    class Index implements IndexAbstraction {

        private final IndexMetadata concreteIndex;
        private final DataStream dataStream;

        public Index(IndexMetadata indexMetadata, DataStream dataStream) {
            this.concreteIndex = indexMetadata;
            this.dataStream = dataStream;
        }

        public Index(IndexMetadata indexMetadata) {
            this(indexMetadata, null);
        }

        @Override
        public String getName() {
            return concreteIndex.getIndex().getName();
        }

        @Override
        public Type getType() {
            return Type.CONCRETE_INDEX;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return List.of(concreteIndex);
        }

        @Override
        public IndexMetadata getWriteIndex() {
            return concreteIndex;
        }

        @Override
        public DataStream getParentDataStream() {
            return dataStream;
        }

        @Override
        public boolean isHidden() {
            return INDEX_HIDDEN_SETTING.get(concreteIndex.getSettings());
        }

        @Override
        public boolean isSystem() {
            return concreteIndex.isSystem();
        }
    }

    /**
     * Represents an alias and groups all {@link IndexMetadata} instances sharing the same alias name together.
     */
    class Alias implements IndexAbstraction {

        private final String aliasName;
        private final List<IndexMetadata> referenceIndexMetadatas;
        private final IndexMetadata writeIndex;
        private final boolean isHidden;
        private final boolean dataStreamAlias;

        public Alias(AliasMetadata aliasMetadata, List<IndexMetadata> indices) {
            this.aliasName = aliasMetadata.getAlias();
            this.referenceIndexMetadatas = indices;

            List<IndexMetadata> writeIndices = indices.stream()
                .filter(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).writeIndex()))
                .collect(Collectors.toList());

            if (writeIndices.isEmpty() && referenceIndexMetadatas.size() == 1
                && referenceIndexMetadatas.get(0).getAliases().get(aliasName).writeIndex() == null) {
                writeIndices.add(referenceIndexMetadatas.get(0));
            }

            if (writeIndices.size() == 0) {
                this.writeIndex = null;
            } else if (writeIndices.size() == 1) {
                this.writeIndex = writeIndices.get(0);
            } else {
                List<String> writeIndicesStrings = writeIndices.stream()
                    .map(i -> i.getIndex().getName()).collect(Collectors.toList());
                throw new IllegalStateException("alias [" + aliasName + "] has more than one write index [" +
                    Strings.collectionToCommaDelimitedString(writeIndicesStrings) + "]");
            }

            this.isHidden = aliasMetadata.isHidden() == null ? false : aliasMetadata.isHidden();
            dataStreamAlias = false;
            validateAliasProperties();
        }

        public Alias(org.elasticsearch.cluster.metadata.DataStreamAlias dataStreamAlias,
                     List<IndexMetadata> indicesOfAllDataStreams,
                     IndexMetadata writeIndexOfWriteDataStream) {
            this.aliasName = dataStreamAlias.getName();
            this.referenceIndexMetadatas = indicesOfAllDataStreams;
            this.writeIndex = writeIndexOfWriteDataStream;
            this.isHidden = false;
            this.dataStreamAlias = true;
        }

        @Override
        public Type getType() {
            return Type.ALIAS;
        }

        @Override
        public boolean isDataStreamRelated() {
            return dataStreamAlias;
        }

        public String getName() {
            return aliasName;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return referenceIndexMetadatas;
        }

        @Nullable
        public IndexMetadata getWriteIndex() {
            return writeIndex;
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
            return referenceIndexMetadatas.stream().allMatch(IndexMetadata::isSystem);
        }

        private void validateAliasProperties() {
            // Validate hidden status
            final Map<Boolean, List<IndexMetadata>> groupedByHiddenStatus = referenceIndexMetadatas.stream()
                .collect(Collectors.groupingBy(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).isHidden())));
            if (isNonEmpty(groupedByHiddenStatus.get(true)) && isNonEmpty(groupedByHiddenStatus.get(false))) {
                List<String> hiddenOn = groupedByHiddenStatus.get(true).stream()
                    .map(idx -> idx.getIndex().getName()).collect(Collectors.toList());
                List<String> nonHiddenOn = groupedByHiddenStatus.get(false).stream()
                    .map(idx -> idx.getIndex().getName()).collect(Collectors.toList());
                throw new IllegalStateException("alias [" + aliasName + "] has is_hidden set to true on indices [" +
                    Strings.collectionToCommaDelimitedString(hiddenOn) + "] but does not have is_hidden set to true on indices [" +
                    Strings.collectionToCommaDelimitedString(nonHiddenOn) + "]; alias must have the same is_hidden setting " +
                    "on all indices");
            }

            // Validate system status

            final Map<Boolean, List<IndexMetadata>> groupedBySystemStatus = referenceIndexMetadatas.stream()
                .collect(Collectors.groupingBy(IndexMetadata::isSystem));
            // If the alias has either all system or all non-system, then no more validation is required
            if (isNonEmpty(groupedBySystemStatus.get(false)) && isNonEmpty(groupedBySystemStatus.get(true))) {
                final List<String> newVersionSystemIndices = groupedBySystemStatus.get(true).stream()
                    .filter(i -> i.getCreationVersion().onOrAfter(IndexNameExpressionResolver.SYSTEM_INDEX_ENFORCEMENT_VERSION))
                    .map(i -> i.getIndex().getName())
                    .sorted() // reliable error message for testing
                    .collect(Collectors.toList());

                if (newVersionSystemIndices.isEmpty() == false) {
                    final List<String> nonSystemIndices = groupedBySystemStatus.get(false).stream()
                        .map(i -> i.getIndex().getName())
                        .sorted() // reliable error message for testing
                        .collect(Collectors.toList());
                    throw new IllegalStateException("alias [" + aliasName + "] refers to both system indices " + newVersionSystemIndices +
                        " and non-system indices: " + nonSystemIndices + ", but aliases must refer to either system or" +
                        " non-system indices, not both");
                }
            }
        }

        private boolean isNonEmpty(List<IndexMetadata> idxMetas) {
            return (Objects.isNull(idxMetas) || idxMetas.isEmpty()) == false;
        }
    }

    class DataStream implements IndexAbstraction {

        private final org.elasticsearch.cluster.metadata.DataStream dataStream;
        private final List<IndexMetadata> dataStreamIndices;
        private final IndexMetadata writeIndex;

        public DataStream(org.elasticsearch.cluster.metadata.DataStream dataStream, List<IndexMetadata> dataStreamIndices) {
            this.dataStream = dataStream;
            this.dataStreamIndices = List.copyOf(dataStreamIndices);
            this.writeIndex =  dataStreamIndices.get(dataStreamIndices.size() - 1);
        }

        @Override
        public String getName() {
            return dataStream.getName();
        }

        @Override
        public Type getType() {
            return Type.DATA_STREAM;
        }

        @Override
        public boolean isDataStreamRelated() {
            return true;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return dataStreamIndices;
        }

        public IndexMetadata getWriteIndex() {
            return writeIndex;
        }

        @Override
        public DataStream getParentDataStream() {
            // a data stream cannot have a parent data stream
            return null;
        }

        @Override
        public boolean isHidden() {
            return dataStream.isHidden();
        }

        @Override
        public boolean isSystem() {
            return dataStream.isSystem();
        }

        public org.elasticsearch.cluster.metadata.DataStream getDataStream() {
            return dataStream;
        }
    }

}
