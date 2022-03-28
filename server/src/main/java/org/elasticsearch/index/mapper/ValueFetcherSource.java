/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.List;

/**
 * Resolves a {@link ValueFetcher} for a particular field.
 */
public interface ValueFetcherSource {
    ValueFetcher preferStored();

    ValueFetcher preferStoredOrEmpty();

    ValueFetcher forceDocValues();

    /**
     * Fields that can be loaded from {@code _source} or doc values.
     */
    abstract class SourceOrDocValues implements ValueFetcherSource {
        private final SearchExecutionContext context;
        private final MappedFieldType ft;
        private final String format;

        public SourceOrDocValues(SearchExecutionContext context, MappedFieldType ft, String format) {
            this.context = context;
            this.ft = ft;
            this.format = format;
        }

        @Override
        public final ValueFetcher preferStored() {
            if (context.isSourceEnabled()) {
                return forceSource();
            }
            if (ft.hasDocValues()) {
                return forceDocValues();
            }
            throw new IllegalArgumentException(
                "Unable to retrieve values because _source is disabled in the mappings for index ["
                    + context.index().getName()
                    + "] and the field doesn't have doc values"
            );
        }

        @Override
        public ValueFetcher preferStoredOrEmpty() {
            if (context.isSourceEnabled()) {
                return forceSource();
            }
            if (ft.hasDocValues()) {
                return forceDocValues();
            }
            return (lookup, ignored) -> List.of();
        }

        @Nullable
        protected abstract ValueFetcher forceSource();

        @Override
        public DocValueFetcher forceDocValues() {
            return new DocValueFetcher(ft.docValueFormat(format, null), context.getForField(ft));
        }
    }

    /**
     * Fields that can only be loaded from doc values.
     */
    final class DocValuesOnly implements ValueFetcherSource {
        private final SearchExecutionContext context;
        private final MappedFieldType ft;
        private final String format;

        public DocValuesOnly(SearchExecutionContext context, MappedFieldType ft, String format) {
            this.context = context;
            this.ft = ft;
            this.format = format;
        }

        @Override
        public ValueFetcher preferStored() {
            if (ft.hasDocValues()) {
                return forceDocValues();
            }
            throw new IllegalArgumentException("Unable to retrieve values because the field doesn't have doc values");
        }

        @Override
        public ValueFetcher preferStoredOrEmpty() {
            if (ft.hasDocValues()) {
                return forceDocValues();
            }
            return (lookup, ignored) -> List.of();
        }

        @Override
        public DocValueFetcher forceDocValues() {
            return new DocValueFetcher(ft.docValueFormat(format, null), context.getForField(ft));
        }
    }

    /**
     * Fields that can only be loaded from source.
     */
    abstract class SourceOnly implements ValueFetcherSource {
        private final SearchExecutionContext context;
        private final MappedFieldType ft;

        public SourceOnly(SearchExecutionContext context, MappedFieldType ft) {
            this.context = context;
            this.ft = ft;
        }

        @Override
        public final ValueFetcher preferStored() {
            if (context.isSourceEnabled()) {
                return forceSource();
            }
            throw new IllegalArgumentException(
                "Unable to retrieve values because _source is disabled in the mappings for index [" + context.index().getName() + "]"
            );
        }

        @Override
        public final ValueFetcher preferStoredOrEmpty() {
            if (context.isSourceEnabled()) {
                return forceSource();
            }
            return (lookup, ignored) -> List.of();
        }

        @Nullable
        protected abstract ValueFetcher forceSource();

        @Override
        public DocValueFetcher forceDocValues() {
            throw new UnsupportedOperationException(
                "[" + ft.name() + "] may only be fetched from _source because it is of type [" + ft.typeName() + "]"
            );
        }
    }

    /**
     * Fields that can only be loaded from a lucene stored field.
     */
    final class StoredOnly implements ValueFetcherSource {
        private final SearchExecutionContext context;
        private final MappedFieldType ft;

        public StoredOnly(SearchExecutionContext context, MappedFieldType ft) {
            this.context = context;
            this.ft = ft;
        }

        @Override
        public ValueFetcher preferStored() {
            if (ft.isStored()) {
                return forceStored();
            }
            throw new IllegalArgumentException(
                "Unable to retrieve values because the field is not configured as stored in the mapping for the index ["
                    + context.index().getName()
                    + "]"
            );
        }

        @Override
        public ValueFetcher preferStoredOrEmpty() {
            if (ft.isStored()) {
                return forceStored();
            }
            return (lookup, ignored) -> List.of();
        }

        private ValueFetcher forceStored() {
            return new StoredValueFetcher(context.lookup(), ft.name());
        }

        @Override
        public DocValueFetcher forceDocValues() {
            throw new UnsupportedOperationException(
                "[" + ft.name() + "] may only be fetched from stored fields because it is of type [" + ft.typeName() + "]"
            );
        }
    }

    /**
     * Constant fields.
     */
    final class Constant implements ValueFetcherSource {
        private final List<Object> values;

        public Constant(List<Object> values) {
            this.values = values;
        }

        @Override
        public ValueFetcher preferStored() {
            return forceDocValues();
        }

        @Override
        public ValueFetcher preferStoredOrEmpty() {
            return forceDocValues();
        }

        @Override
        public ValueFetcher forceDocValues() {
            return (lookup, ignored) -> values;
        }
    }
}
