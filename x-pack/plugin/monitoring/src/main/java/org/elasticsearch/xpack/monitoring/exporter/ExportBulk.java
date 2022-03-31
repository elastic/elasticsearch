/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * An export bulk holds one of more documents until it got flushed. The {@link ExportBulk#flush(ActionListener)} usually triggers the
 * exporting of the documents to their final destination.
 */
public abstract class ExportBulk {

    protected final String name;
    protected final ThreadContext threadContext;
    /**
     * {@code closed} being {@code false} means that it can still be added onto.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    public ExportBulk(String name, ThreadContext threadContext) {
        this.name = Objects.requireNonNull(name);
        this.threadContext = Objects.requireNonNull(threadContext);
    }

    /**
     * Get the name used for any logging messages.
     *
     * @return Never {@code null}.
     */
    public String getName() {
        return name;
    }

    /**
     * Add documents to the exporting bulk
     */
    public void add(Collection<MonitoringDoc> docs) throws ExportException {
        if (closed.get() == false) {
            doAdd(docs);
        }
    }

    protected abstract void doAdd(Collection<MonitoringDoc> docs) throws ExportException;

    /**
     * Flush the exporting bulk
     */
    public void flush(ActionListener<Void> listener) {
        if (closed.compareAndSet(false, true)) {
            doFlush(listener);
        } else {
            listener.onResponse(null);
        }
    }

    protected abstract void doFlush(ActionListener<Void> listener);

    /**
     * This class holds multiple export bulks exposed as a single compound bulk.
     */
    public static class Compound extends ExportBulk {

        private final List<ExportBulk> bulks;

        public Compound(List<ExportBulk> bulks, ThreadContext threadContext) {
            super("all", threadContext);
            this.bulks = bulks;
        }

        @Override
        protected void doAdd(Collection<MonitoringDoc> docs) throws ExportException {
            ExportException exception = null;
            for (ExportBulk bulk : bulks) {
                try {
                    bulk.add(docs);
                } catch (ExportException e) {
                    if (exception == null) {
                        exception = new ExportException("failed to add documents to export bulks", e);
                    }
                    exception.addExportException(e);
                }
            }
            if (exception != null) {
                throw exception;
            }
        }

        @Override
        protected void doFlush(ActionListener<Void> listener) {
            final SetOnce<ExportException> exceptionRef = new SetOnce<>();
            final BiConsumer<ExportBulk, ActionListener<Void>> bulkBiConsumer = (exportBulk, iteratingListener) -> {
                // for every export bulk we flush and pass back the response, which should always be
                // null. When we have an exception, we wrap the first and then add suppressed exceptions
                exportBulk.flush(ActionListener.wrap(iteratingListener::onResponse, e -> {
                    if (exceptionRef.get() == null) {
                        exceptionRef.set(new ExportException("failed to flush export bulks", e));
                    } else if (e instanceof ExportException) {
                        exceptionRef.get().addExportException((ExportException) e);
                    } else {
                        exceptionRef.get().addSuppressed(e);
                    }
                    // this is tricky to understand but basically we suppress the exception for use
                    // later on and call the passed in listener so that iteration continues
                    iteratingListener.onResponse(null);
                }));
            };
            IteratingActionListener<Void, ExportBulk> iteratingActionListener = new IteratingActionListener<>(
                newExceptionHandlingListener(exceptionRef, listener),
                bulkBiConsumer,
                bulks,
                threadContext
            );
            iteratingActionListener.run();
        }

        private static ActionListener<Void> newExceptionHandlingListener(
            SetOnce<ExportException> exceptionRef,
            ActionListener<Void> listener
        ) {
            return ActionListener.wrap(r -> {
                if (exceptionRef.get() == null) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(exceptionRef.get());
                }
            }, listener::onFailure);
        }
    }

}
