/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

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
    List<Index> getIndices();

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

    default Index getWriteIndex(IndexRequest request, Metadata metadata) {
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
     * @return the names of aliases referring to this instance.
     *         Returns <code>null</code> if aliases can't point to this instance.
     */
    @Nullable
    List<String> getAliases();

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

        private final Index concreteIndexName;
        private final boolean isHidden;
        private final boolean isSystem;
        private final List<String> aliases;
        private final DataStream dataStream;

        public ConcreteIndex(IndexMetadata indexMetadata, DataStream dataStream) {
            this.concreteIndexName = indexMetadata.getIndex();
            this.isHidden = indexMetadata.isHidden();
            this.isSystem = indexMetadata.isSystem();
            this.aliases = indexMetadata.getAliases() != null ? List.of(indexMetadata.getAliases().keys().toArray(String.class)) : null;
            this.dataStream = dataStream;
        }

        public ConcreteIndex(IndexMetadata indexMetadata) {
            this(indexMetadata, null);
        }

        @Override
        public String getName() {
            return concreteIndexName.getName();
        }

        @Override
        public Type getType() {
            return Type.CONCRETE_INDEX;
        }

        @Override
        public List<Index> getIndices() {
            return List.of(concreteIndexName);
        }

        @Override
        public Index getWriteIndex() {
            return concreteIndexName;
        }

        @Override
        public DataStream getParentDataStream() {
            return dataStream;
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
        public List<String> getAliases() {
            return aliases;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConcreteIndex that = (ConcreteIndex) o;
            return isHidden == that.isHidden
                && isSystem == that.isSystem
                && concreteIndexName.equals(that.concreteIndexName)
                && Objects.equals(aliases, that.aliases)
                && Objects.equals(dataStream, that.dataStream);
        }

        @Override
        public int hashCode() {
            return Objects.hash(concreteIndexName, isHidden, isSystem, aliases, dataStream);
        }
    }

    /**
     * Represents an alias and groups all {@link IndexMetadata} instances sharing the same alias name together.
     */
    class Alias implements IndexAbstraction {

        private final String aliasName;
        private final List<Index> referenceIndexMetadatas;
        private final Index writeIndex;
        private final boolean isHidden;
        private final boolean isSystem;
        private final boolean dataStreamAlias;

        public Alias(AliasMetadata aliasMetadata, List<IndexMetadata> indices) {
            this.aliasName = aliasMetadata.getAlias();
            this.referenceIndexMetadatas = new ArrayList<>(indices.size());
            for (IndexMetadata imd : indices) {
                this.referenceIndexMetadatas.add(imd.getIndex());
            }

            List<IndexMetadata> writeIndices = indices.stream()
                .filter(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).writeIndex()))
                .collect(Collectors.toList());

            if (writeIndices.isEmpty() && indices.size() == 1 && indices.get(0).getAliases().get(aliasName).writeIndex() == null) {
                writeIndices.add(indices.get(0));
            }

            if (writeIndices.size() == 0) {
                this.writeIndex = null;
            } else if (writeIndices.size() == 1) {
                this.writeIndex = writeIndices.get(0).getIndex();
            } else {
                List<String> writeIndicesStrings = writeIndices.stream().map(i -> i.getIndex().getName()).collect(Collectors.toList());
                throw new IllegalStateException(
                    "alias ["
                        + aliasName
                        + "] has more than one write index ["
                        + Strings.collectionToCommaDelimitedString(writeIndicesStrings)
                        + "]"
                );
            }

            this.isHidden = aliasMetadata.isHidden() == null ? false : aliasMetadata.isHidden();
            this.isSystem = indices.stream().allMatch(IndexMetadata::isSystem);
            dataStreamAlias = false;
            validateAliasProperties(indices);
        }

        public Alias(DataStreamAlias dataStreamAlias, List<Index> indicesOfAllDataStreams, Index writeIndexOfWriteDataStream) {
            this.aliasName = dataStreamAlias.getName();
            this.referenceIndexMetadatas = indicesOfAllDataStreams;
            this.writeIndex = writeIndexOfWriteDataStream;
            this.isHidden = false;
            this.isSystem = false;
            this.dataStreamAlias = true;
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
            return referenceIndexMetadatas;
        }

        @Nullable
        public Index getWriteIndex() {
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
            return isSystem;
        }

        @Override
        public boolean isDataStreamRelated() {
            return dataStreamAlias;
        }

        @Override
        public List<String> getAliases() {
            return null;
        }

        private void validateAliasProperties(List<IndexMetadata> referenceIndexMetadatas) {
            // Validate hidden status
            final Map<Boolean, List<IndexMetadata>> groupedByHiddenStatus = referenceIndexMetadatas.stream()
                .collect(Collectors.groupingBy(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).isHidden())));
            if (isNonEmpty(groupedByHiddenStatus.get(true)) && isNonEmpty(groupedByHiddenStatus.get(false))) {
                List<String> hiddenOn = groupedByHiddenStatus.get(true)
                    .stream()
                    .map(idx -> idx.getIndex().getName())
                    .collect(Collectors.toList());
                List<String> nonHiddenOn = groupedByHiddenStatus.get(false)
                    .stream()
                    .map(idx -> idx.getIndex().getName())
                    .collect(Collectors.toList());
                throw new IllegalStateException(
                    "alias ["
                        + aliasName
                        + "] has is_hidden set to true on indices ["
                        + Strings.collectionToCommaDelimitedString(hiddenOn)
                        + "] but does not have is_hidden set to true on indices ["
                        + Strings.collectionToCommaDelimitedString(nonHiddenOn)
                        + "]; alias must have the same is_hidden setting "
                        + "on all indices"
                );
            }

            // Validate system status

            final Map<Boolean, List<IndexMetadata>> groupedBySystemStatus = referenceIndexMetadatas.stream()
                .collect(Collectors.groupingBy(IndexMetadata::isSystem));
            // If the alias has either all system or all non-system, then no more validation is required
            if (isNonEmpty(groupedBySystemStatus.get(false)) && isNonEmpty(groupedBySystemStatus.get(true))) {
                final List<String> newVersionSystemIndices = groupedBySystemStatus.get(true)
                    .stream()
                    .filter(i -> i.getCreationVersion().onOrAfter(IndexNameExpressionResolver.SYSTEM_INDEX_ENFORCEMENT_VERSION))
                    .map(i -> i.getIndex().getName())
                    .sorted() // reliable error message for testing
                    .collect(Collectors.toList());

                if (newVersionSystemIndices.isEmpty() == false) {
                    final List<String> nonSystemIndices = groupedBySystemStatus.get(false)
                        .stream()
                        .map(i -> i.getIndex().getName())
                        .sorted() // reliable error message for testing
                        .collect(Collectors.toList());
                    throw new IllegalStateException(
                        "alias ["
                            + aliasName
                            + "] refers to both system indices "
                            + newVersionSystemIndices
                            + " and non-system indices: "
                            + nonSystemIndices
                            + ", but aliases must refer to either system or"
                            + " non-system indices, not both"
                    );
                }
            }
        }

        private boolean isNonEmpty(List<IndexMetadata> idxMetas) {
            return (Objects.isNull(idxMetas) || idxMetas.isEmpty()) == false;
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
                && referenceIndexMetadatas.equals(alias.referenceIndexMetadatas)
                && Objects.equals(writeIndex, alias.writeIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aliasName, referenceIndexMetadatas, writeIndex, isHidden, isSystem, dataStreamAlias);
        }
    }

    class DataStream implements IndexAbstraction {

        public static final XContentParserConfiguration TS_EXTRACT_CONFIG = XContentParserConfiguration.EMPTY.withFiltering(
            Set.of("@timestamp"),
            null
        );

        private final org.elasticsearch.cluster.metadata.DataStream dataStream;
        private final List<String> referencedByDataStreamAliases;

        public DataStream(org.elasticsearch.cluster.metadata.DataStream dataStream, List<String> aliases) {
            this.dataStream = dataStream;
            this.referencedByDataStreamAliases = aliases;
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
        public List<Index> getIndices() {
            return dataStream.getIndices();
        }

        public Index getWriteIndex() {
            return dataStream.getWriteIndex();
        }

        @Override
        public Index getWriteIndex(IndexRequest request, Metadata metadata) {
            if (request.opType() != DocWriteRequest.OpType.CREATE) {
                return getWriteIndex();
            }

            if (getType() != IndexAbstraction.Type.DATA_STREAM) {
                return getWriteIndex();
            }

            if (dataStream.isTimeSeries(metadata::index) == false) {
                return getWriteIndex();
            }

            Instant timestamp;
            XContent xContent = request.getContentType().xContent();
            try (XContentParser parser = xContent.createParser(TS_EXTRACT_CONFIG, request.source().streamInput())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                switch (parser.nextToken()) {
                    case VALUE_STRING:
                        // TODO: deal with nanos too here.
                        // (the index hasn't been resolved yet, keep track of timestamp field metadata at data stream level,
                        // so we can use it here)
                        timestamp = Instant.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(parser.text()));
                        break;
                    case VALUE_NUMBER:
                        timestamp = Instant.ofEpochMilli(parser.longValue());
                        break;
                    default:
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(
                                Locale.ROOT,
                                "Failed to parse object: expecting token of type [%s] or [%s] but found [%s]",
                                XContentParser.Token.VALUE_STRING,
                                XContentParser.Token.VALUE_NUMBER,
                                parser.currentToken()
                            )
                        );
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Error extracting timestamp: " + e.getMessage(), e);
            }
            Index result = dataStream.selectTimeSeriesWriteIndex(timestamp, metadata);
            if (result == null) {
                String timestampAsString = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(timestamp);
                String writeableIndicesString = dataStream.getIndices()
                    .stream()
                    .map(metadata::index)
                    .map(IndexMetadata::getSettings)
                    .map(
                        settings -> "["
                            + settings.get(IndexSettings.TIME_SERIES_START_TIME.getKey())
                            + ","
                            + settings.get(IndexSettings.TIME_SERIES_END_TIME.getKey())
                            + "]"
                    )
                    .collect(Collectors.joining());
                throw new IllegalArgumentException(
                    "the document timestamp ["
                        + timestampAsString
                        + "] is outside of ranges of currently writable indices ["
                        + writeableIndicesString
                        + "]"
                );
            }
            return result;
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

        @Override
        public boolean isDataStreamRelated() {
            return true;
        }

        @Override
        public List<String> getAliases() {
            return referencedByDataStreamAliases;
        }

        public org.elasticsearch.cluster.metadata.DataStream getDataStream() {
            return dataStream;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataStream that = (DataStream) o;
            return dataStream.equals(that.dataStream) && Objects.equals(referencedByDataStreamAliases, that.referencedByDataStreamAliases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStream, referencedByDataStreamAliases);
        }
    }

}
