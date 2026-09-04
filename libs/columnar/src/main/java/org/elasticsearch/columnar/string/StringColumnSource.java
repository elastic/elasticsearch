/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/**
 * Binary doc values that can hand over the string column behind them, so a search matches a term against
 * the column rather than reading a value for every document.
 *
 * <p>Doc values reach a search through whatever wraps them, and what a caller holds is not always the
 * instance the format made. Asking for this rather than for that instance lets anything standing in front
 * of a column offer what the column can do, by implementing this and answering from what it wraps.
 * Doc values that do not offer it are read a document at a time, which every binary doc values answers.
 */
public interface StringColumnSource {

    /** The column behind these values. */
    StringColumnReader reader();
}
