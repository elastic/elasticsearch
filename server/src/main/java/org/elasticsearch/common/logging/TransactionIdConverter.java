/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.tasks.Task;

import java.util.Objects;

/**
 * Pattern converter to format the transaction id provided in the traceparent header into JSON fields <code>transaction.id</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "TransactionIdConverter")
@ConverterKeys({"transaction_id"})
public final class TransactionIdConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static TransactionIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new TransactionIdConverter();
    }

    public TransactionIdConverter() {
        super("transaction_id", "transaction_id");
    }

    public static String getTransactionId() {
        return HeaderWarning.THREAD_CONTEXT.stream()
            .map(t -> t.<String>getTransient(Task.TRANSACTION_ID))
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    /**
     * Formats the transaction.id into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the clusterId value
     *              from <code>NodeAndClusterIdStateListener</code> to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        String transactionId = getTransactionId();
        if (transactionId != null) {
            toAppendTo.append(transactionId);
        }
    }

}
