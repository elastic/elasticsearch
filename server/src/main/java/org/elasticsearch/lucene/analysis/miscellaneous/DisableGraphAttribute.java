/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.Attribute;

/**
 * This attribute can be used to indicate that the {@link PositionLengthAttribute}
 * should not be taken in account in this {@link TokenStream}.
 * Query parsers can extract this information to decide if this token stream should be analyzed
 * as a graph or not.
 */
public interface DisableGraphAttribute extends Attribute {}
