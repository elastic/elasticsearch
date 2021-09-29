/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;

/**
 * Individual operation to perform on the cluster state as part of an {@link IndicesAliasesRequest}.
 */
public abstract class AliasAction {
    private final String index;

    private AliasAction(String index) {
        if (false == Strings.hasText(index)) {
            throw new IllegalArgumentException("[index] is required");
        }
        this.index = index;
    }

    /**
     * Get the index on which the operation should act.
     */
    public String getIndex() {
        return index;
    }

    /**
     * Should this action remove the index? Actions that return true from this will never execute
     * {@link #apply(NewAliasValidator, Metadata.Builder, IndexMetadata)}.
     */
    abstract boolean removeIndex();

    /**
     * Apply the action.
     *
     * @param aliasValidator call to validate a new alias before adding it to the builder
     * @param metadata metadata builder for the changes made by all actions as part of this request
     * @param index metadata for the index being changed
     * @return did this action make any changes?
     */
    abstract boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index);

    /**
     * Validate a new alias.
     */
    @FunctionalInterface
    public interface NewAliasValidator {
        void validate(String alias, @Nullable String indexRouting, @Nullable String filter, @Nullable Boolean writeIndex);
    }

    /**
     * Operation to add an alias to an index.
     */
    public static class Add extends AliasAction {
        private final String alias;

        @Nullable
        private final String filter;

        @Nullable
        private final String indexRouting;

        @Nullable
        private final String searchRouting;

        @Nullable
        private final Boolean writeIndex;

        @Nullable final Boolean isHidden;

        /**
         * Build the operation.
         */
        public Add(String index, String alias, @Nullable String filter, @Nullable String indexRouting, @Nullable String searchRouting,
                   @Nullable Boolean writeIndex, @Nullable Boolean isHidden) {
            super(index);
            if (false == Strings.hasText(alias)) {
                throw new IllegalArgumentException("[alias] is required");
            }
            this.alias = alias;
            this.filter = filter;
            this.indexRouting = indexRouting;
            this.searchRouting = searchRouting;
            this.writeIndex = writeIndex;
            this.isHidden = isHidden;
        }

        /**
         * Alias to add to the index.
         */
        public String getAlias() {
            return alias;
        }

        public Boolean writeIndex() {
            return writeIndex;
        }

        @Nullable
        public Boolean isHidden() {
            return isHidden;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            aliasValidator.validate(alias, indexRouting, filter, writeIndex);

            AliasMetadata newAliasMd = AliasMetadata.newAliasMetadataBuilder(alias).filter(filter).indexRouting(indexRouting)
                    .searchRouting(searchRouting).writeIndex(writeIndex).isHidden(isHidden).build();

            // Check if this alias already exists
            AliasMetadata currentAliasMd = index.getAliases().get(alias);
            if (currentAliasMd != null && currentAliasMd.equals(newAliasMd)) {
                // It already exists, ignore it
                return false;
            }

            metadata.put(IndexMetadata.builder(index).putAlias(newAliasMd));
            return true;
        }
    }

    /**
     * Operation to remove an alias from an index.
     */
    public static class Remove extends AliasAction {
        private final String alias;
        @Nullable
        private final Boolean mustExist;

        /**
         * Build the operation.
         */
        public Remove(String index, String alias, @Nullable Boolean mustExist) {
            super(index);
            if (false == Strings.hasText(alias)) {
                throw new IllegalArgumentException("[alias] is required");
            }
            this.alias = alias;
            this.mustExist = mustExist;
        }

        /**
         * Alias to remove from the index.
         */
        public String getAlias() {
            return alias;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            if (false == index.getAliases().containsKey(alias)) {
                if (mustExist != null && mustExist) {
                    throw new ResourceNotFoundException("required alias [" + alias  + "] does not exist");
                }
                return false;
            }
            metadata.put(IndexMetadata.builder(index).removeAlias(alias));
            return true;
        }
    }

    /**
     * Operation to remove an index. This is an "alias action" because it allows us to remove an index at the same time as we remove add an
     * alias to replace it.
     */
    public static class RemoveIndex extends AliasAction {
        public RemoveIndex(String index) {
            super(index);
        }

        @Override
        boolean removeIndex() {
            return true;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            throw new UnsupportedOperationException();
        }
    }

    public static class AddDataStreamAlias extends AliasAction {

        private final String aliasName;
        private final String dataStreamName;
        private final Boolean isWriteDataStream;
        private final String filter;

        public AddDataStreamAlias(String aliasName, String dataStreamName, Boolean isWriteDataStream, String filter) {
            super(dataStreamName);
            this.aliasName = aliasName;
            this.dataStreamName = dataStreamName;
            this.isWriteDataStream = isWriteDataStream;
            this.filter = filter;
        }

        public String getAliasName() {
            return aliasName;
        }

        public String getDataStreamName() {
            return dataStreamName;
        }

        public Boolean getWriteDataStream() {
            return isWriteDataStream;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            aliasValidator.validate(aliasName, null, filter, isWriteDataStream);
            return metadata.put(aliasName, dataStreamName, isWriteDataStream, filter);
        }
    }

    public static class RemoveDataStreamAlias extends AliasAction {

        private final String aliasName;
        private final Boolean mustExist;
        private final String dataStreamName;

        public RemoveDataStreamAlias(String aliasName, String dataStreamName, Boolean mustExist) {
            super(dataStreamName);
            this.aliasName = aliasName;
            this.mustExist = mustExist;
            this.dataStreamName = dataStreamName;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            boolean mustExist = this.mustExist != null ? this.mustExist : false;
            return metadata.removeDataStreamAlias(aliasName, dataStreamName, mustExist);
        }
    }
}
