/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;

/**
 * Read state of the producer instance that opens a field's ordinal doc values, handed to
 * {@link SortedOrdinalReader#ordinals} and {@link SortedSetOrdinalReader#ordinals}.
 *
 * <p>Unlike {@link NumericReadContext}, which is fixed for the segment, one of these exists per producer
 * instance: a merge instance owns a cloned data input and builds its blocked ordinals in merge mode, so it
 * carries its own context instead of sharing the original producer's.
 *
 * @param data          the data input of the producer opening the doc values; a reader that needs
 *                      independent sequential access must clone it
 * @param maxDoc        the document count of the segment
 * @param blockOrdinals builds the doc values of fields stored in the blocked ordinal layout
 */
public record OrdinalReadContext(IndexInput data, int maxDoc, BlockOrdinalsFactory blockOrdinals) {}
