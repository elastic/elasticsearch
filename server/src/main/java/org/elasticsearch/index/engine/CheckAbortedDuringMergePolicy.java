/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.Strings.format;

/**
 * A policy that checks if a merge is aborted before accessing segment files to merge.
 */
public class CheckAbortedDuringMergePolicy extends OneMergeWrappingMergePolicy {

    private interface Checker {

        void ensureNotAborted() throws MergeAbortedException;

        void ensureNotAborted(CheckedRunnable<IOException> runnable, String method) throws IOException;
    }

    public static final Setting<Boolean> ENABLE_CHECK_ABORTED_DURING_MERGE = Setting.boolSetting(
        "index.merge.check_aborted_during_merge_policy.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.ServerlessPublic
    );

    private static final Logger logger = LogManager.getLogger(CheckAbortedDuringMergePolicy.class);

    public CheckAbortedDuringMergePolicy(final ShardId shardId, final MergePolicy delegate, LongSupplier relativeTimeInMillis) {
        super(delegate, toWrap -> new OneMerge(toWrap) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                return wrapReader(shardId, toWrap.wrapForMerge(reader), this, relativeTimeInMillis);
            }
        });
    }

    private static CodecReader wrapReader(ShardId shardId, CodecReader reader, OneMerge oneMerge, LongSupplier relativeTimeInMillis) {
        return new CheckingCodecReader(logger, shardId, reader, oneMerge, relativeTimeInMillis);
    }

    private static class CheckingCodecReader extends FilterCodecReader implements Checker {

        private final Logger logger;
        private final ShardId shardId;
        private final OneMerge oneMerge;
        private final LongSupplier relativeTimeInMillis;

        private CheckingCodecReader(
            Logger logger,
            ShardId shardId,
            CodecReader delegate,
            OneMerge oneMerge,
            LongSupplier relativeTimeInMillis
        ) {
            super(delegate);
            this.logger = logger;
            this.shardId = shardId;
            this.oneMerge = oneMerge;
            this.relativeTimeInMillis = relativeTimeInMillis;
        }

        @Override
        public final void ensureNotAborted() throws MergeAbortedException {
            try {
                oneMerge.checkAborted();
            } catch (MergeAbortedException e) {
                logger.debug(() -> format("%s merge is aborted", shardId));
                throw e;
            }
        }

        @Override
        public final void ensureNotAborted(CheckedRunnable<IOException> runnable, String method) throws IOException {
            logger.debug(() -> format("%s %s - start", shardId, method));
            boolean executed = false;
            long startTimeInMillis = relativeTimeInMillis.getAsLong();
            try {
                ensureNotAborted();
                runnable.run();
                executed = true;
            } finally {
                long endTimeInMillis = relativeTimeInMillis.getAsLong();
                boolean success = executed;
                logger.debug(
                    () -> format("%s %s - end (success: %s, took: {} ms)", shardId, method, success, (endTimeInMillis - startTimeInMillis))
                );
            }
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return getDelegate().getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return getDelegate().getReaderCacheHelper();
        }

        @Override
        public StoredFieldsReader getFieldsReader() {
            return new CheckingStoredFieldsReader(super.getFieldsReader(), this);
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            return new CheckingDocValuesProducer(super.getDocValuesReader(), this);
        }

        @Override
        public void checkIntegrity() throws IOException {
            ensureNotAborted(() -> super.checkIntegrity(), "CodecReader#checkIntegrity");
        }
    }

    private static class CheckingStoredFieldsReader extends StoredFieldsReader {

        private final StoredFieldsReader delegate;
        private final Checker checker;
        private long visitedDocs;

        private CheckingStoredFieldsReader(StoredFieldsReader delegate, Checker checker) {
            this.delegate = delegate;
            this.checker = checker;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            if (visitedDocs % 100L == 0L) {
                checker.ensureNotAborted();
            }
            delegate.document(docID, visitor);
            visitedDocs += 1L;
        }

        @Override
        public StoredFieldsReader clone() {
            return new CheckingStoredFieldsReader(delegate.clone(), checker);
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            return new CheckingStoredFieldsReader(delegate.getMergeInstance(), checker);
        }

        @Override
        public void checkIntegrity() throws IOException {
            checker.ensureNotAborted(() -> delegate.checkIntegrity(), "StoredFieldsReader#checkIntegrity");
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class CheckingDocValuesProducer extends FilterDocValuesProducer {

        private final Checker checker;
        private long visitedFields;

        protected CheckingDocValuesProducer(DocValuesProducer delegate, Checker checker) {
            super(delegate);
            this.checker = checker;
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            if (visitedFields % 100L == 0L) {
                checker.ensureNotAborted();
            }
            var values = super.getNumeric(field);
            visitedFields += 1;
            return values;
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            if (visitedFields % 100L == 0L) {
                checker.ensureNotAborted();
            }
            var values = super.getBinary(field);
            visitedFields += 1;
            return values;
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            if (visitedFields % 100L == 0L) {
                checker.ensureNotAborted();
            }
            var values = super.getSorted(field);
            visitedFields += 1;
            return values;
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            if (visitedFields % 100L == 0L) {
                checker.ensureNotAborted();
            }
            var values = super.getSortedNumeric(field);
            visitedFields += 1;
            return values;
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            if (visitedFields % 100L == 0L) {
                checker.ensureNotAborted();
            }
            var values = super.getSortedSet(field);
            visitedFields += 1;
            return values;
        }

        @Override
        public void checkIntegrity() throws IOException {
            checker.ensureNotAborted(() -> super.checkIntegrity(), "DocValuesProducer#checkIntegrity");
        }
    }
}
