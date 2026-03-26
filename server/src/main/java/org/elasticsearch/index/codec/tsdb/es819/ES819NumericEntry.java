/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;

/**
 * ES819 numeric entry. Currently identical to the base type; exists so that
 * the ES819 {@link org.elasticsearch.index.codec.tsdb.EntryFactory} returns
 * a codec-specific type for every entry kind.
 */
final class ES819NumericEntry extends AbstractTSDBDocValuesProducer.NumericEntry {}
